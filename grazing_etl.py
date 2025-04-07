import polars, os, logging
import psycopg2 as pg
from psycopg2 import sql
from numpy import ndarray
from datetime import datetime
from yaml import load, Loader
from img2table.document import PDF
from img2table.ocr import TesseractOCR

logging.basicConfig(
    filename="log/grazing_etl.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

user = os.getenv("kwb_dw_user")
host = os.getenv("kwb_dw_host")
password = os.getenv("kwb_dw_password")


def extract_data(file: str, ocr: TesseractOCR) -> polars.DataFrame:
    """
    This transforms tables from pdf files into usable data using ocr

    Args:
        file: str, file to be processed
        ocr: TesseractOCR, Optical Character Recognition software that
            processes PDF table data. First it transforms the pdf into
            an image and then processes that image to create tabular data
    Returns:
        polars.DataFrame, data to be loaded
    """
    try:
        pdf = PDF(f"{file}", pages=[0], detect_rotation=False, pdf_text_extraction=True)
        table = pdf.extract_tables(ocr=ocr, implicit_rows=True, implicit_columns=False)
        logging.info("Table successly extracted from %s." % (file))
        return polars.from_pandas(table[0][0].df)
    except:
        logging.exception("")


def transform_data(data: polars.DataFrame, etl_yaml: dict):
    """
    This cleans and processes the output of the ocr into loadable data

    Args:
        data: polars.DataFrame,
        etl_yaml: dict, general variables for the etl process
    Returns:
        polars.DataFrame, cleaned data to be loaded
    """
    try:
        # tables' first two rows only contain grazer and reporting date info
        # we'll choose the remaining rows without the total (we can calculate that)
        # and add the grazer/reporting date values as separate columns
        grazer = data[0, 1]
        reporting_date = data[1, 1]
        data = data[3:-1]

        # rename/reorder columns, add new columns
        data.columns = ["area", "number_of_animal_units", "comments"]
        data = data.with_columns(
            polars.lit(grazer).alias("grazer"),
            polars.lit(reporting_date).alias("reporting_date"),
        )
        data = data[etl_yaml["db_schema"]].drop_nulls(
            subset=["area", "number_of_animal_units"]
        )
        data = data.with_columns(
            polars.col("area").replace(etl_yaml["areas_to_replace"]),
            polars.col("reporting_date").str.to_date(format="%B %d, %Y"),
        )
        logging.info("Data successfully transformed.")
        return data
    except:
        logging.exception("")


def get_pg_connection(db_name: str) -> pg.extensions.connection:
    """
    This tests a connection with a postgres database to ensure that
    we're loading into a database that actually exists.

    Args:
        db_name: str, name of database to connect to.
    Returns:
        con: pg.extensions.connection, psycopg connection to pg database
    """
    try:
        con = pg.connect(
            "dbname=%s user=%s host=%s password=%s" % (db_name, user, host, password)
        )
        con.autocommit = True
        logging.info("Successfully connected to %s db" % (db_name))
        return con

    except pg.OperationalError as Error:
        logging.error(Error)


def check_table_exists(con: pg.extensions.connection, schema_name: str, table: str):
    """
    This tests a to ensure the table we'll be writing to exists in
    the postgres schema provided.

    Args:
        con: pg.extensions.connection, psycopg connection to pg
            database
        schema_name: str, name of postgres schema
        table_name: str, name of table
    """
    cur = con.cursor()
    command = sql.SQL(
        """
        Select * from {schema_name}.{table} limit 1  
        """
    ).format(
        schema_name=sql.Identifier(schema_name),
        table=sql.Identifier(table),
    )
    try:
        cur.execute(command)
        if isinstance(cur.fetchall(), list):
            logging.info("Table exists, continue with loading.")
    except pg.OperationalError as Error:
        logging.error(Error)


def load_data_into_pg_warehouse(data: polars.DataFrame, etl_yaml: dict):
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: polars.DataFrame, data to be loaded into warehouse
        etl_yaml: dict, general variables for the etl process
    """
    con = get_pg_connection(etl_yaml["db_name"])
    check_table_exists(con, etl_yaml["schema_name"], etl_yaml["table_name"])
    try:
        cur = con.cursor()
        for row in data.to_numpy():
            query = build_load_query(row, etl_yaml)
            cur.execute(query)
        cur.close()
        con.close()
        logging.info(
            "Data was successfully loaded to %s.%s.%s"
            % (
                etl_yaml["db_name"],
                etl_yaml["schema_name"],
                etl_yaml["table_name"],
            )
        )
    except pg.OperationalError as Error:
        con.close()
        logging.error(Error)
    return


def build_load_query(data: ndarray, etl_yaml: dict) -> pg.sql.Composed:
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: numpy.ndarray, row of data to be loaded
        etl_yaml: dict, variables for the etl process
    Returns:
        pg.sql.Composed, Upsert query used to load data
    """
    col_names = sql.SQL(", ").join(sql.Identifier(col) for col in etl_yaml["db_schema"])
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table} ({col_names}) VALUES ({values})
        ON CONFLICT ({prim_key}) DO UPDATE SET {update_col} = Excluded.{update_col}, edited_on = current_timestamp;
        """
    ).format(
        schema_name=sql.Identifier(etl_yaml["schema_name"]),
        table=sql.Identifier(etl_yaml["table_name"]),
        col_names=col_names,
        values=values,
        prim_key=sql.SQL(etl_yaml["prim_key"]),
        update_col=sql.Identifier(etl_yaml["update_col"]),
    )


if __name__ == "__main__":
    logger.info(
        "--------------- Grazing ETL ran on %s ----------------" % (datetime.today())
    )
    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.")
        exit()
    etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)
    ocr = TesseractOCR(n_threads=1, lang="eng", psm=6)

    combined_data = []
    for file in os.listdir("data_dump"):
        raw_data = extract_data(f"data_dump/{file}", ocr=ocr)
        cleaned_data = transform_data(raw_data, etl_yaml)
        combined_data.append(cleaned_data)

    proc_data = polars.concat(combined_data)
    proc_data.write_parquet(etl_yaml["transformed_parquet"])

    load_data_into_pg_warehouse(
        data=proc_data,
        etl_yaml=etl_yaml,
    )

    os.rename(
        etl_yaml["transformed_parquet"],
        "loaded_data/grazing_data_loaded_%s.parquet"
        % (datetime.date(datetime.today())),
    )
    logger.info("Succesfully ran Grazing ETL.\n")
