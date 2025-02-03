****************
Analysis Flavors
****************

A key concept in `rail_projects` are analysis `Flavors`, which are versions of
similar analyses with slightly different parameter settings and/or
input files.

=============
Flavor basics
=============

A `RailProject` will contain several `Flavors` and once special `Flavor`, called `Baseline`,
which is included in all the other `Flavors`.

When various `RailProject` functions are called, the used can specifiy which `Flavors` should be
used. `RailProject` also has tools to make it easy to iterate over several `Flavors`, or even all
the available flavors, and call the functions for each one,


==================
Flavor definitions
==================


The parameters neeed to define a `Flavor` are given :py:class:`rail.projects.project.RailFlavor`

.. code-block:: python
       
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Flavor name"),
        catalog_tag=StageParameter(
            str, None, fmt="%s", msg="tag for catalog being used"
        ),
        pipelines=StageParameter(list, ["all"], fmt="%s", msg="pipelines being used"),
        file_aliases=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
        pipeline_overrides=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
    )


These are:    

a `name` for the variant, used to construct filenames

a `catalog_tag`, which identifies format of the data being used, and sets
the expected names of columns accordingly

a list of `pipelines` that can be run in this variant

a list of `file_aliases` that can be used to specify the
input files used in this variant

a dict of `pipeline_overrides` that modify the behavior of the various pipelines


Here is an example:

.. code-block:: yaml

  # Baseline configuraiton, included in others by default
  Baseline:
    catalog_tag: roman_rubin
    pipelines: ['all']
    file_aliases:  # Set the training and test files
      test: test_file_100k
      train: train_file_100k      
      train_zCOSMOS: train_file_zCOSMOS_100k

  # These define the variant configurations for the various parts of the analysis
  Flavors:
    - Flavor:
        name: train_cosmos
        pipelines: ['pz', 'tomography']  # only run the pz and tomography pipelines
        file_aliases:  # Set the training and test files
          test: test_file_100k
          train: train_file_zCOSMOS_100k		
    - Flavor:
        name: gpz_gl
        pipelines: ['pz']  # only run the pz pipeline
        pipeline_overrides:  # Override specifics for particular pipelines
          default:
            kwargs:
              algorithms: ['gpz']  # Only run gpz
          pz:  # overrides for 'inform' pipline
            inform_gpz:  # overides for the 'inform_gpz' stage
              gpz_method: GL

