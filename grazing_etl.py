import polars, os, logging
from kwb_loader import loader
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


def extract_data(file):
    try:
        pdf = PDF(f"{file}", pages=[0], detect_rotation=False, pdf_text_extraction=True)
        table = pdf.extract_tables(ocr=ocr, implicit_rows=True, implicit_columns=False)
        logging.info("Table successly extracted from pdf %s." % (file))
        return polars.from_pandas(table[0][0].df)
    except:
        logging.exception("")


def transform_data(
    data: polars.DataFrame, columns_in_order: list, areas_to_replace: dict
):
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
    data = data[columns_in_order].drop_nulls(subset=["area", "number_of_animal_units"])
    data = data.with_columns(
        polars.col("area").replace(areas_to_replace),
        polars.col("reporting_date").str.to_date(format="%B %d, %Y"),
    )
    logging.info("Data successfully transformed.")
    return data


if __name__ == "__main__":
    logger.info(
        "--------------- Grazing ETL ran on %s ----------------" % (datetime.today())
    )

    ocr = TesseractOCR(n_threads=1, lang="eng", psm=6)

    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.")
        exit()
    etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)
    combined_data = []
    for file in os.listdir("data_dump"):

        raw_data = extract_data(f"data_dump/{file}")
        cleaned_data = transform_data(
            raw_data, etl_yaml["columns_in_order"], etl_yaml["areas_to_replace"]
        )
        combined_data.append(cleaned_data)
    polars.concat(combined_data).write_parquet(etl_yaml["transformed_parquet"])
    loader.load(
        credentials=(user, host, password),
        dbname=etl_yaml["db_name"],
        schema=etl_yaml["schema"],
        table_name=etl_yaml["table"],
        data_path=etl_yaml["transformed_parquet"],
        prim_key=etl_yaml["prim_key"],
    )
    logging.info(
        "Successfully loaded data into %s.%s.%s \n"
        % (etl_yaml["db_name"], etl_yaml["schema"], etl_yaml["table"])
    )

    os.rename(
        etl_yaml["transformed_parquet"],
        "loaded_data/grazing_data_loaded_%s.parquet"
        % (datetime.date(datetime.today())),
    )
