# Installing the Command Line Interface (CLI)

We interact with Blurr using a Command Line Interface (CLI). Blurr is installed via pip:

`$ pip install blurr`

To check the installation was correct:

`$ blurr --version`

The help command lists all the commands:

`$ blurr --help`

# Working with DTCs

DTCs are the central part of blurr. There are several CLI commands to validate_schema_spec and test DTCs

## Validate

The validate_schema_spec command will check whether a DTC is valid.

`$ blurr validate_schema_spec weekly_rollup.dtc`

## Transform

The transform command will apply the DTC to a source of event data.

Example:

```
$ blurr transform \
     --streaming-dtc ./dtcs/sessionize-dtc.yml \
     --window-dtc ./dtcs/windowing-dtc.yml \
     --source file://path
```

## Debug

Print logs for debugging using the `--debug` option

`blurr transform --streaming-dtc=streaming-dtc.yml --window-dtc=window-dtc.yml raw-data-window.json --debug`

## Supported data sources

Valid sources of data are:

Type | Description | URI example
---- | ----------- | -----------
`text` | A text file | `file://<path>`
`internet` | An internet resource | http://blurr.ai/data-example.log
`S3` | AWS S3 location | `s3://bucket/key`

## Supported data destinations

Transformed data is output to `stdout`. Data destination details are defined inside the DTC.

More destinations will be supported in the future, please create [an issue](https://github.com/productml/blurr/issues/new) to request for a specific data destination! Or better yet, contribute to Blurr and build it!

# Deploying Blurr on AWS

The deploy command is used to rollout an instance of Blurr. Currently, only AWS is supported.

`$ blurr deploy config.json`

## Configuration File

Blurr configuration can become a bit verbose, and some data formats (such as lists or dicts) are not supported in the command line. For those cases, the CLI allows passing a JSON configuration file with all the required parameters:

```
{
   "dtcs":{
      "include":"./*-dtc.yml"
   },
   "sources":[
      {
         "type":"s3",
         "uri":"s3://bucket/key"
      }
   ]}
```

Available options are:

`dtcs`: The set of DTCs to be part of the deployment

`sources`: URIs for the data sources to be transformed (TODO: how many sources can we define in the config?)

## The deploy command

Blurr gets deployed as a Cloudformation Stack. After a successful deployment we should see a stack named Blurr in the Cloudformation console.

TODO: add image

We can override the name of the stack using the `--stack-name` option:

`$ blurr deploy config.json --stack-name blurr-staging`

We can check the configuration of a blurr stack at any time using the config command:

`$ blurr config --stack-name blurr-staging`

If you feel more comfortable deploying Blurr by yourself it is possible to export the Cloudformation template from the command line:

`$ blurr export-cfn config.json --out cfn-template.zip`

## Updating Configuration

Configuration can be updated using `update-config` command:

`$ blurr update-config --stack-name blurr-prod`

TODO: don't we need to provide the new config as an argument?
