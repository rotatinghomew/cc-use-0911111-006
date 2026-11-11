from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from registry_delta_hub.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from registry_delta_hub.feeds.packet_reader import build_processor as packet_reader_factory
from registry_delta_hub.identity.resolution import build_processor as resolution_factory
from registry_delta_hub.temporal.version_merge import build_processor as version_merge_factory
from registry_delta_hub.quality.gates import build_processor as gates_factory
from registry_delta_hub.ledger.registry_store import build_processor as registry_store_factory
from registry_delta_hub.aggregation.population_rollup import build_processor as population_rollup_factory
from registry_delta_hub.recovery.quarantine import build_processor as quarantine_factory
from registry_delta_hub.publication.release_bundle import build_processor as release_bundle_factory

PROCESSOR_FACTORIES = [packet_reader_factory, resolution_factory, version_merge_factory, gates_factory, registry_store_factory, population_rollup_factory, quarantine_factory, release_bundle_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='registry_delta_hub')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'registry_delta_hub',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        record["registry_key"]="|".join([record.get("region",""),record.get("subject_id",""),record.get("category","")])
        record["revision_vector"]={record.get("source_system","unknown"):int(record.get("revision",0))}
        record["merge_policy"]="latest-event-then-highest-source-revision"
        return record

    def architecture_plan(self, ctx):
        ctx.emit("registry_microbatch","vector_opened",vector=ctx.payload["revision_vector"])
        plan=[]
        for index,processor in enumerate(self.processors):
            plan.append(processor)
            if index in {1,2}: ctx.emit("registry_microbatch","identity_temporal_barrier",ordinal=index)
        return plan

    def architecture_finalize(self, ctx):
        ctx.payload["registry_release"]={"key":ctx.payload["registry_key"],"quality_flags":len(ctx.flags),"rollup_ready":not any(x.startswith("invalid:") for x in ctx.flags)}
        ctx.metrics["registry_completeness"]=round(len(ctx.evidence)/max(1,len(self.processors)*38),6)
        ctx.emit("registry_release","microbatch_sealed",release=ctx.payload["registry_release"])

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
