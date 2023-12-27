# CHANGELOG
## 0.3.35
- [x] add: validations for the input file for conversation tagging

## 0.3.34
- [x] PL-61: Add retry mechanism for uploading data to Label studio

## 0.3.33
- [x] PL-1300: new data format support & extra meta-annotation-columns  (#21)

## 0.3.32
- [x] fix: alternatives column postprocessing after downloading from labelstudio

## 0.3.31

- [x] add: required paramter --data-label for uploading datasets
## 0.3.30

- [x] add: modified downloading from db to integrate labelstudio db queries


## 0.3.29

- [x] fix: data format issue after downloading from labelstudio

## 0.3.28

- [x] fix: pass project_id for LS and job_id for TOG

## 0.3.27

- [x] add: makes batches of batched_dataset while uploading to tog db - limits max async connections to db at a time
- [x] add: retries with sleep while uploading data to tog db

## 0.3.26

- [x] fix: bug(#13) passing of creds into creating Job as None

## 0.3.25

- [x] fix: datetime formatting produces `None` if none of the formats match.
- [x] fix: connection timeout mitigated by batching the requests.
- [x] update: data download uses start and end date for query only if provided.

## 0.3.24

- [x] add: error handling for uploads.

## 0.3.23

- [x] add: job-id to dataset.
- [x] add: json cols are unpacked.

## 0.3.22

- [x] feat: Add labelstudio integration. We can upload/download datasets to labelstudio.

## 0.3.21

- [x] update: Dedupe id uses uuid4 at runtime.

## 0.3.20

- [x] fix: Parse timezone from strings.

## 0.3.18

- [x] add: Support for legacy dataframes with alternatives.

## 0.3.17

- [x] update: gh actions script updates.

## 0.3.16

- [x] update: Data upload is more tolerant or utterance json vs python object.

## 0.3.15

- [x] update: JSON fields are stringified.

## 0.3.13

- [x] update: describe and stat dataset requires database object instance optionally.

## 0.3.11

- [x] update: deps for compatibility with skit-pipelines.


## 0.3.10

- [x] update: Higher tolerance for dataset schema errors.

## 0.3.9

- [x] fix: -j added back to upload dataset command.

## 0.3.8

- [x] update: remove preprocessing over df.

## 0.3.7 

- [x] fix: bug preventing data uploads due to larger than supported values in source.

## 0.3.6

- [x] update: dataset {stat, describe} can apply date ranges.

## 0.3.5

- [x] update: CLI allows database params as input.

## 0.3.4

- [x] update: obtain type of the dataset from `Job::type()`.

## 0.3.3

- [x] refactor: no difference in usage.

## 0.3.2

- [x] refactor: dependency on skit-fixdf removed.

## 0.3.1

- [x] feat: Upload datasets via cli.

## 0.3.0

- [x] feat: Access datasets from dvc.
