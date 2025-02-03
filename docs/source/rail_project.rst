***********
RailProject
***********

==================
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


==========================
Rail Project Functionality
==========================

Source code: :py:class:`rail.projects.project.RailProject`

Once the analysis setup and analysis flavors are defined,
most of what users will do comes down to running a small set of
functions in `RailProject`, which we describe here.



RailProject.load_config
-----------------------

Source code: :func:`rail.projects.project.RailProject.load_config`

Read a yaml file and create a RailProject


RailProject.reduce_data
-----------------------

Source code: :func:`rail.projects.project.RailProject.reduce_data`

Make a reduced catalog from an input catalog by applying a selction
and trimming unwanted colums.  This is run before the analysis pipelines.


RailProject.subsample_data
--------------------------

Source code: :func:`rail.projects.project.RailProject.subsample_data`

Subsample data from a catalog to make a testing or training file.
This is run after catalog level pipelines, but before pipelines run
on indvidudal training/ testing samples.


RailProject.build_pipelines
---------------------------

Source code: :func:`rail.projects.project.RailProject.build_pipelines`

Build ceci pipeline yaml files for a particular set of analysis
flavors.  This will build all the pipelines that are defined for that
analysis flavor.


RailProject.run_pipeline_single
-------------------------------

Source code: :func:`rail.projects.project.RailProject.run_pipeline_single`

Run a pipeline on a single file for a specific analysis flavor.
This will require the user to specify any additional interpolants,
such as the selection name, that are needed to uniqule specify the
input files.



RailProject.run_pipeline_catalog
--------------------------------

Source code: :func:`rail.projects.project.RailProject.run_pipeline_catalog`

Run a pipeline on a catalog of files for a specific analysis flavor.
This will require the user to specify any additional interpolants,
such as the selection name, that are needed to uniqule specify the
input files.


==========================
Rail Project Configuration
==========================

Most of these element come from the shared library of elements,
which is accesible from :py:mod:`rail.projects.library` module.


Rail Project shared configuration files
---------------------------------------

`Includes: list[str]`

List of shared configuration files to load


Rail Project analysis flavor definitions
----------------------------------------

See :ref:`Flavor definitions` or
:py:class:`rail.projects.project.RailFlavor` for the parameters needed to define an
analysis `Flavor`. 


`Baseline: dict[str, Any]`

Baseline configuration for this project.
This is included in all the other analysis flavors


`Flavors: list[dict[str, Any]]`

List of all the analysis flavors that have been defined in this project



Rail Project bookkeeping elements
---------------------------------

These are used to define the file paths for the project.

`PathTemplates: dict[str, str]`

Overrides for templates used to construct file paths.  The defaults
are given in :py:mod:`rail.projects.name_utils`

.. code-block:: python

    PathTemplates = dict(
        pipeline_path="{pipelines_dir}/{pipeline}_{flavor}.yaml",
        ceci_output_dir="{project_dir}/data/{selection}_{flavor}",
        ceci_file_path="{tag}_{stage}.{suffix}",
    )



`CommonPaths: dict[str, str]`

Defintions of common paths used to construct file paths.  The defaults
are given in :py:mod:`rail.projects.name_utils`

.. code-block:: python

    CommonPaths = dict(
        root=".",          # needs to be overridden
        scratch_root=".",  # needs to be overridden
        project="",        # needs to be overridden
        project_dir="{root}/projects/{project}",
        project_scratch_dir="{scratch_root}/projects/{project}",
        catalogs_dir="{root}/catalogs",
        pipelines_dir="{project_dir}/pipelines",
    )


`IterationVars: dict[str, list[str]]`

Iteration variables to construct the catalogs.  For example, the
roman-rubin catalog is split by healpix pixel, and to get the whole
catalog you have to iterate over all the healpix pixels, so this would
look like

.. code-block:: yaml

    IterationVars:
        healpix: [all_the_pixels]


Note that if you want to set up a project to only use some of the
available data, that is prefectly fine.  All you have to do is shorten
the list.

		 


Rail Project shared elements
----------------------------

Things that are pulled from the library, each of these is just a list
of the names of things that are defined in the library that
can be used in this project.  The default is to use all the
items defined in the library.

`Catalogs: list[str] = ['all']` These are actually CatalogTemplates

`Files: list[str] = ['all']` These are actually FileTemplates

`Pipelines: list[str] = ['all']` These are actually PipelineTemplates

`Reducers: list[str] = ['all']` These reduce the input data catalog

`Subsamplers: list[str] = ['all'] These subsample catalogs to get individual
files

`Selections: list[str] = ['all']` These are the selection parameters

`Subsamples: list[str] = ['all']` These are the subsample parameters

`PZAlgorithms: list[str] = ['all']`

`SpecSelections: list[str] = ['all']`

`Classifiers: list[str] = ['all']`

`Summarizers: list[str] = ['all']`

`ErrorModels: list[str] = ['all']`
