from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib.parse

# password = 'postgres'

# encoded_password = urllib.parse.quote_plus(password)

# SQLALCHEMY_DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/shellsan"

# engine = create_engine(SQLALCHEMY_DATABASE_URL)

# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base = declarative_base()



password = 'ab45cd12'

encoded_password = urllib.parse.quote_plus(password)

# SQLALCHEMY_DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/shellv2"


SQLALCHEMY_DATABASE_URL = f"postgresql://postgres:{encoded_password}@localhost:5432/appr22shell"

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()