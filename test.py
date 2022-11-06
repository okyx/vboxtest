from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import re
import os
if os.path.exists('count.txt-00000-of-00002'):
    os.remove('count.txt-00000-of-00001')
    os.remove('count.txt-00001-of-00002')
output_path = 'count.txt'
beam_options = PipelineOptions([
   "--runner=PortableRunner",
    "--job_endpoint=192.168.0.165:8099"
    "--environtment_type=LOOPBACK"
])

with beam.Pipeline() as p:
    (
        p
        | beam.io.ReadFromText('text.txt')
        | beam.Map(lambda x:x.lower())
        | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | beam.combiners.Count.PerElement()
        | beam.io.WriteToText('count.txt')
    )