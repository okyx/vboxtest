from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
output_path = 'count.txt'
beam_options = PipelineOptions([
   "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK",
])
with beam.Pipeline(options= beam_options) as p:
    (
        p
        | beam.Create(range(100))
        | beam.Map(lambda x : 'genap' if x%2==0 else 'ganjil')
        | beam.combiners.Count.PerElement()
        # | beam.Map(lambda x: (x,1))
        # | beam.CombinePerKey(sum)
        | beam.Map(print)
    )