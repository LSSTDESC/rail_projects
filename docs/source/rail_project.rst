***********
RailProject
***********


RailProject basics
==================

:py:class:`rail.projects.project.RailProject` is the main user facing class.

It collects all the elements needed to
run a collection of studies using RAIL.

The key concepts are:

1. analysis `Flavors`, which are versions of
   similar analyses with slightly different parameter settings and/or
   input files.

2. ceci `Pipelines`, which run blocks of analysis code

A `RailProject` basically specifies which `Pipelines` to run under which
`Flavors`, and keeps track of the outputs.



Rail Project Functionality
==========================


RailProject.load_config
-----------------------

Read a yaml file and create a RailProject


RailProject.reduce_data
-----------------------

Make a reduced catalog from an input catalog by applying a selction
and trimming unwanted colums.  This is run before the analysis pipelines.


RailProject.subsample_data
--------------------------

Subsample data from a catalog to make a testing or training file.
This is run after catalog level pipelines, but before pipeliens run
on indvidudal training/ testing samples.


RailProject.build_pipelines
---------------------------

Build ceci pipeline yaml files.


RailProject.run_pipeline_single
-------------------------------

Run a pipeline on a single file


RailProject.run_pipeline_catalog
--------------------------------

Run a pipeline on a catalog of files


Rail Project Configuration
==========================

Most of these element come from the shared library of elements,
which is accesible from rail.projects.library

Rail Project shared configuration files
---------------------------------------

`Includes: list[str]`

List of shared configuration files to load


Rail Project analysis flavors
-----------------------------

`Baseline: dict[str, Any]`

Baseline configuration for this project.
This is included in all the other analysis flavors


`Flavors: list[dict[str, Any]]`

List of all the analysis flavors that have been defined in this project


Rail Project bookkeeping elements
---------------------------------

These used to define the file paths for the project.

`PathTemplates: dict[str, str]`

Overrides for templates used to construct file paths


`CommonPaths: dict[str, str]`

Defintions of common paths used to construct file paths


`IterationVars: dict[str, list[str]]`

Iteration variables to construct the catalogs


Rail Project shared elements
----------------------------

Things that are pulled from the library, each of these is just a list
of the names of things that are defined in the library that
can be used in this project.  The default is to use all the
items defined in the library.

`Catalogs: list[str]` These are actually CatalogTemplates
`Files: list[str]` These are actually FileTemplates
`Pipelines: list[str]` These are actually PipelineTemplates
`Reducers: list[str]` These reduce the input data catalog
`Subsamplers: list[str]` These subsample catalogs to get individual files
`Selections: list[str]` These are the selection parameters
`Subsamples: list[str]` These are the subsample parameters
`PZAlgorithms: list[str]`
`SpecSelections: list[str]`
`Classifiers: list[str]`
`Summarizers: list[str]`
`ErrorModels: list[str]`
