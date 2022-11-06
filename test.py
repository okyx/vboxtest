from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import re
import os
if os.path.exists('count.txt-00000-of-00001'):
    os.remove('count.txt-00000-of-00001')
output_path = 'count.txt'
beam_options = PipelineOptions([
   "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK",
    # "--environment_config=localhost:50000"
])
with beam.Pipeline() as p:
    (
        p
        | beam.Create(range(100))
        | beam.Map(lambda x : 'genap' if x%2==0 else 'ganjil')
        | beam.combiners.Count.PerElement()
        | beam.Map(print)
    )
with beam.Pipeline() as p:
    (
        p
        | beam.io.ReadFromText('text.txt')
        | beam.Map(lambda x:x.lower())
        | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | beam.combiners.Count.PerElement()
        | beam.io.WriteToText('count.txt')
    )