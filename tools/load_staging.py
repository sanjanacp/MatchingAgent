#!/usr/bin/env python3
"""
Load SEC Reg CF, Reg D, and Form ADV datasets into SQLite staging tables.

The script expects the raw quarterly files under ~/Downloads/data/
and writes a SQLite database at data/staging.sqlite.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "staging.sqlite"
DATA_ROOT = Path.home() / "Downloads" / "data"
SCHEMA_PATH = REPO_ROOT / "data" / "staging_schema.sql"

TRUE_SET = {"true", "t", "1", "y", "yes"}
FALSE_SET = {"false", "f", "0", "n", "no"}


def normalize_bool(series: pd.Series) -> pd.Series:
    def _convert(value: object) -> object:
        if value is None:
            return None
        text = str(value).strip().lower()
        if not text:
            return None
        if text in TRUE_SET:
            return True
        if text in FALSE_SET:
            return False
        return None

    return series.map(_convert, na_action="ignore").astype("boolean")


def parse_date(series: pd.Series, fmt: str | None = None) -> pd.Series:
    if fmt:
        return pd.to_datetime(series, errors="coerce", format=fmt).dt.date
    return pd.to_datetime(series, errors="coerce").dt.date


TABLES = [
    "stg_fd_FORMDSUBMISSION",
    "stg_fd_ISSUERS",
    "stg_fd_OFFERING",
    "stg_cf_FORM_C_SUBMISSION",
    "stg_cf_FORM_C_ISSUER_INFORMATION",
    "stg_cf_FORM_C_DISCLOSURE",
    "stg_cf_FORM_C_ISSUER_JURISDICTIONS",
    "stg_adv_base_a",
    "stg_adv_base_b",
]


def exec_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for table in TABLES:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.close()
    con.executescript(SCHEMA_PATH.read_text())


def load_form_d(con: sqlite3.Connection) -> None:
    base = DATA_ROOT / "2025Q1_d"

    submission = pd.read_csv(base / "FORMDSUBMISSION.tsv", sep="\t", dtype=str)
    submission["FILING_DATE"] = parse_date(submission["FILING_DATE"], fmt="%d-%b-%Y")
    submission = submission[
        ["ACCESSIONNUMBER", "SUBMISSIONTYPE", "FILING_DATE", "FILE_NUM", "SIC_CODE", "TESTORLIVE"]
    ]
    submission.to_sql("stg_fd_FORMDSUBMISSION", con, if_exists="append", index=False)

    issuers = pd.read_csv(base / "ISSUERS.tsv", sep="\t", dtype=str)
    issuers = issuers[
        [
            "ACCESSIONNUMBER",
            "CIK",
            "ENTITYNAME",
            "STREET1",
            "STREET2",
            "ENTITYTYPE",
            "CITY",
            "STATEORCOUNTRY",
            "STATEORCOUNTRYDESCRIPTION",
            "ZIPCODE",
            "JURISDICTIONOFINC",
            "YEAROFINC_VALUE_ENTERED",
            "ISSUERPHONENUMBER",
        ]
    ]
    issuers.to_sql("stg_fd_ISSUERS", con, if_exists="append", index=False)

    offering = pd.read_csv(base / "OFFERING.tsv", sep="\t", dtype=str)
    for col in [
        "ISEQUITYTYPE",
        "ISDEBTTYPE",
        "ISPOOLEDINVESTMENTFUNDTYPE",
        "HASNONACCREDITEDINVESTORS",
    ]:
        offering[col] = normalize_bool(offering[col]).astype("Int64")
    offering["SALE_DATE"] = parse_date(offering["SALE_DATE"], fmt="%Y-%m-%d")
    offering = offering[
        [
            "ACCESSIONNUMBER",
            "INDUSTRYGROUPTYPE",
            "FEDERALEXEMPTIONS_ITEMS_LIST",
            "ISEQUITYTYPE",
            "ISDEBTTYPE",
            "ISPOOLEDINVESTMENTFUNDTYPE",
            "HASNONACCREDITEDINVESTORS",
            "TOTALOFFERINGAMOUNT",
            "TOTALAMOUNTSOLD",
            "TOTALREMAINING",
            "MINIMUMINVESTMENTACCEPTED",
            "SALE_DATE",
        ]
    ]
    offering.to_sql("stg_fd_OFFERING", con, if_exists="append", index=False)


def load_reg_cf(con: sqlite3.Connection) -> None:
    base = DATA_ROOT / "2025Q1_cf"

    submission = pd.read_csv(base / "FORM_C_SUBMISSION.tsv", sep="\t", dtype=str)
    submission["FILING_DATE"] = parse_date(submission["FILING_DATE"], fmt="%Y%m%d")
    submission = submission[
        ["ACCESSION_NUMBER", "SUBMISSION_TYPE", "FILING_DATE", "CIK", "FILE_NUMBER", "PERIOD"]
    ]
    submission.to_sql("stg_cf_FORM_C_SUBMISSION", con, if_exists="append", index=False)

    issuer = pd.read_csv(base / "FORM_C_ISSUER_INFORMATION.tsv", sep="\t", dtype=str)
    issuer["PROGRESSUPDATE"] = issuer["PROGRESSUPDATE"].fillna("")
    issuer = issuer[
        [
            "ACCESSION_NUMBER",
            "NAMEOFISSUER",
            "LEGALSTATUSFORM",
            "JURISDICTIONORGANIZATION",
            "STREET1",
            "STREET2",
            "CITY",
            "STATEORCOUNTRY",
            "ZIPCODE",
            "ISSUERWEBSITE",
            "PROGRESSUPDATE",
        ]
    ]
    issuer.to_sql("stg_cf_FORM_C_ISSUER_INFORMATION", con, if_exists="append", index=False)

    disclosure = pd.read_csv(base / "FORM_C_DISCLOSURE.tsv", sep="\t", dtype=str)
    disclosure["DEADLINEDATE"] = parse_date(disclosure["DEADLINEDATE"], fmt="%Y-%m-%d")
    disclosure = disclosure.rename(
        columns={
            "TOTALASSETPRIORFISCALYEAR": "TOTALASSETPRIORYEAR",
            "REVENUEPRIORFISCALYEAR": "REVENUEPRIORYEAR",
            "NETINCOMEPRIORFISCALYEAR": "NETINCOMEPRIORYEAR",
        }
    )
    disclosure = disclosure[
        [
            "ACCESSION_NUMBER",
            "SECURITYOFFEREDTYPE",
            "NOOFSECURITYOFFERED",
            "PRICE",
            "OFFERINGAMOUNT",
            "MAXIMUMOFFERINGAMOUNT",
            "OVERSUBSCRIPTIONACCEPTED",
            "OVERSUBSCRIPTIONALLOCATIONTYPE",
            "DEADLINEDATE",
            "CURRENTEMPLOYEES",
            "TOTALASSETMOSTRECENTFISCALYEAR",
            "REVENUEMOSTRECENTFISCALYEAR",
            "NETINCOMEMOSTRECENTFISCALYEAR",
            "TOTALASSETPRIORYEAR",
            "REVENUEPRIORYEAR",
            "NETINCOMEPRIORYEAR",
        ]
    ]
    disclosure.to_sql("stg_cf_FORM_C_DISCLOSURE", con, if_exists="append", index=False)

    juris = pd.read_csv(base / "FORM_C_ISSUER_JURISDICTIONS.tsv", sep="\t", dtype=str)
    if "ISSUEJURISDICTIONSECUROFFERING" in juris.columns:
        juris = juris.rename(columns={"ISSUEJURISDICTIONSECUROFFERING": "STATEORPROVINCE"})
    juris["COUNTRY"] = pd.NA
    juris[["ACCESSION_NUMBER", "STATEORPROVINCE", "COUNTRY"]].to_sql(
        "stg_cf_FORM_C_ISSUER_JURISDICTIONS", con, if_exists="append", index=False
    )


def load_adv_base_a(con: sqlite3.Connection) -> None:
    path = DATA_ROOT / "adv-filing-data-20111105-20241231-part1" / "IA_ADV_Base_A_20111105_20241231.csv"
    usecols = [
        "FilingID",
        "DateSubmitted",
        "1A",
        "1F1-Street 1",
        "1F1-Street 2",
        "1F1-City",
        "1F1-State",
        "1F1-Country",
        "1F1-Postal",
        "1F1-Private",
        "1F2-M-F",
        "1F2-Other",
        "1F2-Hours",
        "1F3",
        "1F4",
        "1F5",
        "1G-Street 1",
        "1G-Street 2",
        "1G-City",
        "1G-State",
        "1G-Country",
        "1G-Postal",
        "1G-Private",
        "5D1a",
        "5D1b",
        "5D1e",
        "5D1f",
        "5D2a",
        "5D2b",
        "5D2c",
        "5D2g",
        "5D2h",
        "5D2j",
        "5D2k",
        "5F2a",
        "5F2b",
        "5F2c",
        "5H",
        "5J2",
        "5K1",
        "5K2",
        "5K3",
        "5K4",
        "7A1",
        "7A2",
        "7A6",
        "7A8",
        "7A9",
        "7A10",
        "7A12",
        "7A16",
        "9A1a",
        "9A1b",
        "9A2a",
        "9A2b",
    ]
    numeric_cols: Iterable[str] = [
        "5D1a",
        "5D1b",
        "5D1e",
        "5D1f",
        "5D2a",
        "5D2b",
        "5D2c",
        "5D2g",
        "5D2h",
        "5D2j",
        "5D2k",
        "5F2a",
        "5F2b",
        "5F2c",
        "9A2a",
        "9A2b",
    ]
    bool_cols = ["5K1", "5K2", "5K3", "5K4", "7A1", "7A2", "7A6", "7A8", "7A9", "7A10", "7A12", "7A16", "9A1a", "9A1b"]

    chunks = pd.read_csv(
        path,
        usecols=usecols,
        encoding="latin1",
        dtype=str,
        chunksize=100_000,
        low_memory=False,
    )
    frames = []
    for chunk in chunks:
        chunk["DateSubmitted"] = parse_date(chunk["DateSubmitted"], fmt="%m/%d/%Y %I:%M:%S %p")
        for col in numeric_cols:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
        for col in bool_cols:
            chunk[col] = normalize_bool(chunk[col]).astype("Int64")
        frames.append(chunk)

    df = pd.concat(frames, ignore_index=True)
    df.to_sql("stg_adv_base_a", con, if_exists="append", index=False)


def load_adv_base_b(con: sqlite3.Connection) -> None:
    path = DATA_ROOT / "adv-filing-data-20111105-20241231-part1" / "IA_ADV_Base_B_20111105_20241231.csv"
    header = pd.read_csv(path, nrows=0, encoding="latin1").columns.tolist()
    state_cols = [col for col in header if col.startswith("2-")]
    usecols = ["FilingID", "3A", "3A-Other"] + state_cols

    frames = []
    chunks = pd.read_csv(
        path,
        usecols=usecols,
        encoding="latin1",
        dtype=str,
        chunksize=50_000,
        low_memory=False,
    )
    for chunk in chunks:
        chunk = chunk.fillna("")
        chunk["2-SECStateReg"] = chunk[state_cols].apply(
            lambda row: ",".join(col for col in state_cols if row[col].strip().upper() == "Y"),
            axis=1,
        )
        frames.append(chunk[["FilingID", "2-SECStateReg", "3A", "3A-Other"]])

    df = pd.concat(frames, ignore_index=True)
    df.to_sql("stg_adv_base_b", con, if_exists="append", index=False)


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    exec_schema(con)

    load_form_d(con)
    load_reg_cf(con)
    load_adv_base_a(con)
    load_adv_base_b(con)

    con.close()
    print(f"Loaded staging tables into {DB_PATH}")


if __name__ == "__main__":
    main()
