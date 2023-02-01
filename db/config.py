from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import declarative_base, sessionmaker

# Uncomment to run the server with persistent
# Server DB: server.db
engine = create_async_engine('sqlite+aiosqlite:///./server.db', future=True, echo=True)

# Uncomment to run unit tests: 
# Test DB: test.db
# engine = create_async_engine('sqlite+aiosqlite:///./tests/test.db', future=True, echo=True)
async_session = sessionmaker(
    engine, expire_on_commit=True, class_=AsyncSession
)

Base = declarative_base()