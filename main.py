import apache_beam as beam
from apache_beam.io import ReadFromText
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

dengue = (
    pipeline
    | "Dengue dataset read" >> ReadFromText("casos_dengue.txt", skip_header_lines = 1)
    | "From string to list" >> beam.Map(lambda line: line.split("|"))
    | "From list to dictionary" >> beam.Map(lambda element: dict(zip(dengue_columns, element)))
    | "Print results" >> beam.Map(print)
)

pipeline.run()
