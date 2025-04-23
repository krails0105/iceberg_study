from datetime import datetime
from typing import List, Tuple

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Column, DateTime, Integer, text, MetaData
from sqlalchemy.ext.declarative import declarative_base

from models import ModelBase
from config import DBConfig


BBMapperRemoteMeta = MetaData(schema="metadata")
BBMapperRemoteBase = declarative_base(metadata=BBMapperRemoteMeta)


class BBMapperModelBase(ModelBase):
    DB = "T1"
    URL = DBConfig.T1_META_URL


class XrplBBMapModel(BBMapperModelBase, BBMapperRemoteBase):

    __tablename__ = "blocktime_blockheight_mapper_xrpl"

    block_height = Column(Integer, primary_key=True)
    block_time = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    @classmethod
    def upsert_blocktimes(cls, data: List[Tuple[int, datetime]]) -> None:
        with cls.session_scope() as session:
            insert_statement = insert(cls).values([
                {"block_height": block_height, "block_time": block_time, "updated_at": text("NOW()")}
                for block_height, block_time in data
            ])

            session.execute(
                insert_statement.on_conflict_do_update(
                    index_elements=["block_height"],
                    set_={"block_time": insert_statement.excluded.block_time, "updated_at": insert_statement.excluded.updated_at}
                )
            )
