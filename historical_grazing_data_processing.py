from img2table.document import PDF
from img2table.ocr import TesseractOCR
import os
import pandas as pd

columns_in_order = [
    "Grazer",
    "Reporting Date",
    "Area",
    "Number of Animal Units",
    "Comments",
]

areas_to_replace = {
    "(north of Main Area Kern Water Bank Canal)": "Main Area (North of KWB Canal)",
    "(south of Main Area Kern Water Bank Canal)": "Main Area (South of KWB Canal)",
    "(north of Strand Area Kern Water Bank Canal)": "Strand Area (North of KWB Canal)",
    "(south of Strand Area Kern Water Bank Canal)": "Strand Area (South of KWB Canal)",
    "6RXWK Area": "South Area",
    "SoutheDVW Area": "Southeast Area",
    "South Strand Area (south of KWB Canal)": "Strand Area (South of KWB Canal)",
    "North Main Area (north of KWB Canal)": "Main Area (North of KWB Canal)",
    "North Strand Area (north of KWB Canal)": "Strand Area (North of KWB Canal)",
    "South Main Area (north of KWB Canal)": "Main Area (South of KWB Canal)",
    "South Strand Area (north of KWB Canal)": "Strand Area (South of KWB Canal)",
}

dates_to_replace = {
    "Octoberber 31, 2016": "October 31, 2016",
    "Novermber 30, 2016": "November 30, 2016",
    "Februrary 28, 2019": "February 28, 2019",
}

if __name__ == "__main__":
    folder = "Grazing Reports/test/rud"

    # Get files to read in path format
    files_to_read = []
    for root, dir, files in os.walk(folder):
        if not files:
            continue
        for file in files:
            file_path = f"{root}/{file}"
            files_to_read.append(file_path)

    ocr = TesseractOCR(n_threads=1, lang="eng", psm=6)
    tables = []
    for file in files_to_read:
        pdf = PDF(f"{file}", pages=[0], detect_rotation=False, pdf_text_extraction=True)
        table = pdf.extract_tables(ocr=ocr, implicit_rows=True, implicit_columns=False)
        table_df = table[0][0].df

        # tables' first two rows only contain grazer and reporting date info
        # we'll choose the remaining rows without the total (we can calculate that)
        # and add the grazer/reporting date values as separate columns
        grazer = table_df.iloc[0, 1]
        reporting_date = table_df.iloc[1, 1]
        table_df = table_df.iloc[3:-1]

        # rename/reorder columns, add new columns
        table_df.columns = ["Area", "Number of Animal Units", "Comments"]
        table_df["Area"] = table_df["Area"].replace("\n", " ", regex=True)
        table_df["Grazer"] = grazer
        table_df["Reporting Date"] = reporting_date
        table_df = table_df[columns_in_order]
        tables.append(table_df.dropna(subset=["Area", "Number of Animal Units"]))
        del table_df

    combined_table = pd.concat(tables)

    # replace data that doesn't match the desired format
    for key, value in areas_to_replace.items():
        combined_table.replace({"Area": {key: value}}, inplace=True)

    for key, value in dates_to_replace.items():
        combined_table.replace({"Reporting Date": {key: value}}, inplace=True)

    combined_table.replace({"Number of Animal Units": {"0*": 0}}, inplace=True)
    combined_table["Reporting Date"] = combined_table["Reporting Date"].fillna(
        value="January 31, 2024"
    )

    area_replacement_for_july_2024 = {
        "Main Area (North of KWB Canal)": "105",
        "Strand Area (North of KWB Canal)": "167",
        "South Area": "91",
        "Main Area (South of KWB Canal)": "45",
    }
    for area, number in area_replacement_for_july_2024.items():
        flag = (
            (combined_table["Grazer"] == "9L Livestock Company")
            & (combined_table["Reporting Date"] == "July 31, 2024")
            & (combined_table["Number of Animal Units"] == number)
        )
        combined_table.loc[flag, "Area"] = area

    combined_table.reset_index(drop=True).to_csv("historical_grazing_data.csv")
