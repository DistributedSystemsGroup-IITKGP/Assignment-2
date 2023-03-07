from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import declarative_base, sessionmaker
import random
from datetime import datetime


random.seed(datetime.now().timestamp())
# Uncomment to run the server with persistent
# Server DB: server.db
dbName = random.randint(0, 1000000)
engine = create_async_engine('sqlite+aiosqlite:///./brokerIndiv' + str(dbName) + '.db', future=True, echo=True)

# Uncomment to run unit tests: 
# Test DB: test.db
# engine = create_async_engine('sqlite+aiosqlite:///./tests/test.db', future=True, echo=True)
async_session = sessionmaker(
    engine, expire_on_commit=True, class_=AsyncSession
)

Base = declarative_base()