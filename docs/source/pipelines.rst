*********
Pipelines
*********

A key concept in `rail_projects` are ceci `Pipelines`, which run blocks of analysis code using `ceci`.


================
Pipelines basics
================

A `RailProject` will have access to several `PipelineTemplates` and
use these to define the `Pipelines` that it runs.

To do this, it will need some additional information.

1. What `Flavor` to run the `Pipeline` with.  This is specified by the
   user, and will set up the `Pipeline` to expect the correct column
   names.
2. What options to use to construct the `Pipeline`, such as which
   algorihms to use, or additional paramters.  The is done by merging
   default infomation in the `PipelineTemplate` with `Flavor` specific information.
3. Any specific overrides for any of the `Pipeline` stages given in
   the `Flavor` definitinos.
4. How to find the input files.  How this is done depends on the type
   of `Pipeline` and if it is being run on a single file or an entire catalog.
5. Where to write the output data.   How this is done depends on the type
   of `Pipeline` and if it is being run on a single file or an entire catalog.


Running a Pipline of a Catalog
------------------------------
   
When a `Pipeline` is run on a catalog of files, the
`input_catalog_template`, user supplied interpolants and possibly the `input_catalog_basename` parameters are
used to construct list of input files

The `input_catalog_template` should refer to a `CatalogTemplate` that
will give the template for the catalog, e.g.,
`{catalogs_dir}/{project}_{sim_version}/{healpix}/part-0.parquet`.

All of the interpolants must be given in one of there places:

1. The IterationVars block of the `RailProject`
2. The CommonPaths block of the `RailProject`
3. Explicitly in the keyword arguements provided to the call to run the catalog


The `output_catalog_template` is used to define the output directory
in much the same way.


Note that some catalogs have `{basename}` as an interpolant.  Since
`ceci` write all of its output files to the same directory by
default, if we want to, say, create a different version of a
`degraded` catalog by selecting the output of a different degrader, we
can simply do so by picked a different file from the same directory.
We can specifiy this by setting the `input_catalog_basename`
parameter.


Running a Pipline single set of inputs
--------------------------------------

When a `Pipeline` is run on a single set of inputs, the
`input_file_templates` parameter and user supplied interpolants are
used to construct list of input files.

For example, the input_file_templates might look like this:


.. code-block:: yaml

    input_file_templates:
      input_train:
        flavor: baseline
        tag: train
      input_test:
        flavor: baseline
        tag: test


This would specify that the are two inputs `input_train` and
`input_test` and that they should be resolved by getting the `test`
and `train` tags from the `FileAliases` block the current `Flavor`,
and resolving them with using `flavor=baseline` as an interpolant.

This sounds a bit complicated.  The idea here is that this is a
mechanism that allow use to input files created in one `Flavor` by
another `Flavor`.  E.g., we can make testing / training files in the
baseline `Flavor` and then use them in many other `Flavors`.  



====================
Pipeline definitions
====================

Here is an example of a `Pipeline` that we typically only run on catalogs.		

.. code-block:: yaml
		
  - PipelineTemplate:
      name: truth_to_observed
      pipeline_class: rail.pipelines.degradation.truth_to_observed.TruthToObservedPipeline
      input_catalog_template: reduced
      output_catalog_template: degraded
      kwargs:
        error_models: ['all']
        selectors: ['all']
        blending: true


Here is an example of a `Pipeline` that we typically run on individual
input files.

.. code-block:: yaml

  - PipelineTemplate:
      name: pz
      pipeline_class: rail.pipelines.estimation.pz_all.PzPipeline
      input_file_templates:
        input_train:
          flavor: baseline
          tag: train
        input_test:
          flavor: baseline
          tag: test
      kwargs:
        algorithms: ['all']
	


=====================================
Building pipelines with rail.projects
=====================================

.. automethod:: rail.projects.RailProject.build_pipelines		
    :noindex:


====================================
Running pipelines with rail.projects
====================================

.. automethod:: rail.projects.RailProject.run_pipeline_single		
    :noindex:


.. automethod:: rail.projects.RailProject.run_pipeline_catalog
    :noindex:
