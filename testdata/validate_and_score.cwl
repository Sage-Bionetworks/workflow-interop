#!/usr/bin/env cwl-runner
#
# Example validate submission file
#
cwlVersion: v1.0
class: CommandLineTool
baseCommand: python

hints:
  DockerRequirement:
    dockerPull: python:3.7

inputs:

  - id: inputfile
    type: File

arguments:
  - valueFrom: validate.py
  - valueFrom: $(inputs.inputfile)
    prefix: -s
  - valueFrom: results.json
    prefix: -r


requirements:
  - class: InlineJavascriptRequirement
  - class: InitialWorkDirRequirement
    listing:
      - entryname: validate.py
        entry: |
          #!/usr/bin/env python
          import argparse
          import json
          parser = argparse.ArgumentParser()
          parser.add_argument("-r", "--results", required=True, help="validation results")
          parser.add_argument("-s", "--submission_file", help="Submission File")

          args = parser.parse_args()
          
          with open(args.submission_file,"r") as sub_file:
              message = sub_file.read()
          invalid_reasons = []
          prediction_file_status = "VALIDATED"
          if not message.startswith("test"):
              invalid_reasons.append("Submission must have test column")
              prediction_file_status = "INVALID"
          score = 3
          result = {'prediction_file_errors': "\n".join(invalid_reasons),
                    'prediction_file_status': prediction_file_status,
                    'score': score}
          with open(args.results, 'w') as o:
              o.write(json.dumps(result))
     
outputs:

  - id: results
    type: File
    outputBinding:
      glob: results.json
