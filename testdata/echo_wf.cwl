#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
inputs:
  input: string

outputs: []

steps:
  echo:
    run: https://raw.githubusercontent.com/Sage-Bionetworks/workflow-interop/syn-docker/testdata/echo.cwl
    in:
      message: input
    out: []

