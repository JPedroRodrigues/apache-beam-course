import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options = pipeline_options)

dengue_columns = [
    "id",
    "data_iniSE",
    "casos",
    "ibge_code",
    "cidade",
    "uf",
    "cep",
    "latitude",
    "longitude"
]


def convert_dengue_date(line: dict) -> dict:
    line["ano_mes"] = '-'.join(line["data_iniSE"].split('-')[:2])
    return line


def fu_key_tuple(line: dict):
    key = line["uf"]
    return (key, line)


def dengue_cases(element: tuple):
    """
    Receive a tuple in the format (<UF>, [{}, {}...])
    
    Return: a tuple ("UF-YYYY-MM", <Dengue cases number>)
    """

    fu, registers =  element

    for register in registers:
        if (bool(re.search(r'\d', register["casos"]))):
            yield (f"{fu}-{register['ano_mes']}", float(register["casos"]))
        else:
            yield (f"{fu}-{register['ano_mes']}", 0.0)


def create_fu_year_month_rain_key(listLine: list):
    """
    Receives a list in the format ['2015-09-02', '-9999.0', 'PA']
    
    Return: a tuple with a key in the format FU-YYYY-MM and its related rain data
    """

    date, mm, fu = listLine
    new_date = '-'.join(date.split('-')[:2])

    mm = 0.0 if float(mm) < 0 else float(mm)
    return (f"{fu}-{new_date}", mm)


def round_up(line: tuple):
    """
    Receives a tuple following this format ('PA-2015-09', 252.1999999999999)

    Return: a tuple with the number rounded up to one floating point number such as ('PA-2015-09', 252.2)
    """

    key, number = line 
    return (key, round(number, 1))


def has_both_fields(lineTuple: tuple) -> bool:
    """
    Receives a tuple in the format ('CE-2015-01', {'rains': [85.8], 'dengue': [175.0]})
    Returns: True if the tuple don't have empty fiels and False if it has
    """

    key, values = lineTuple
    return all([values["rains"], values["dengue"]])


def decompress_result_elements(element: tuple) -> tuple:
    """
    Receives a tuple in the format ('CE-2015-01', {'rains': [85.8], 'dengue': [175.0]})
    Returns: a tuple in the format ("CE", "2015", "01", "85.8", "175.0")
    """
    key, data = element 
    fu, year, month = key.split('-')
    rain = str(data["rains"][0])
    dengue = str(data["dengue"][0])

    return (fu, year, month, rain, dengue)


def convert_to_csv(lineTuple: tuple, delimiter: str = ';') -> str:
    """
    Receives a tuple in the format ("CE", 2015, 01, 85.8, 175.0)
    Returns: a string in a csv format separated by a given delimiter "CE; 2015; 01; 85.8; 175.0"
    """
    
    return f"{delimiter}".join(lineTuple)


dengue = (
    pipeline
    | "Dengue dataset read" >> ReadFromText("datasets/casos_dengue.txt", skip_header_lines = 1)
    | "From string to list" >> beam.Map(lambda line: line.split("|"))
    | "From list to dictionary" >> beam.Map(lambda element: dict(zip(dengue_columns, element)))
    | "Convert date" >> beam.Map(convert_dengue_date)
    | "Create tuple with FU as a Key" >> beam.Map(fu_key_tuple)
    | "Group by FU" >> beam.GroupByKey()
    | "Decompress dengue cases" >> beam.FlatMap(dengue_cases)
    | "Cases sum by the key FU-YYYY-MM" >> beam.CombinePerKey(sum)
    # | "Print dengue dataset results" >> beam.Map(print)
)


rain = (
    pipeline
    | "Rain dataset read" >> ReadFromText("datasets/chuvas.csv", skip_header_lines = 1)
    | "Tranform rain data string to list" >> beam.Map(lambda line: line.split(","))
    | "Create FU-YYYY-MM key in a tuple" >> beam.Map(create_fu_year_month_rain_key)
    | "Sum rain occurences by the key" >> beam.CombinePerKey(sum)
    | "Round up floating point numbers" >> beam.Map(round_up)
    # | "Print rain dataset results" >> beam.Map(print)
)


result = (
    # (rain, dengue)
    # | "Pile pcollections" >> beam.Flatten()
    # | "Group all the pcollections" >> beam.GroupByKey()
    ({"rains": rain, "dengue": dengue})
    | "Group pcols" >> beam.CoGroupByKey()
    | "Filter empty values" >> beam.Filter(has_both_fields)
    | "Decompress result elements" >> beam.Map(decompress_result_elements)
    | "Convert result line into csv" >> beam.Map(convert_to_csv, delimiter = ',')
    # | "Show the union result" >> beam.Map(print)
)

result | "Create csv file" >> WriteToText("result", file_name_suffix = ".csv", header = "UF,ANO,MÊS,CHUVA,DENGUE")

pipeline.run()
