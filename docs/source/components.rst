************************************
components, factories, and libraries
************************************

**components**

Doing series of related studies using RAIL requires many pieces, such
as the lists of algorithms available, sets of analysis pipelines we
might run, types of plots we might make, types of data we can extract
from out analyses, references to particular files or sets of files we
want to use for out analysses, and so for.   In general we call these
analysis components, and we need ways to keep track of them.

We have implemented interfaces to allow us to read and write
components to yaml files.  


**factories**

A Factory is a python class that can make specific type or types of
components, assign names to each, and keep track of what it has made.


**libraries**:

A library is the collection of all the components that have been
loaded.  Typically there are collected into one, or a few yaml
configuration files to allow users to load them easily.


*******************
Analysis Components
*******************


=========================
Analysis component basics
=========================

An analysis component is our way of wrapping a small bit of the analysis configuration in a way that lets us share and reuse it.

The basic interface to analysis components is the :py:class:`rail.projects.configurable.Configurable` class, which defines a few things,

1. parameters associated to that component, via the ``config_options`` class member, and the expected types of those parameters,
2. type validate, to ensure that objects are created with the correct types of parameters,
3. access to the current values of the parameters via the ``config`` data member,
4. mechansims to read/write the component to yaml, including the ``yaml_tag`` class member defining the yaml tag that marks a block of yaml as defining an object of a particular type of component.


============================
File and Catalog definitions
============================

Objects that define files and sets of files.


FileInstance
------------

:py:class:`rail.projects.file_template.RailProjectFileInstance` just holds the path to a single file and associates that to a simple name.


FileTemplate
------------

:py:class:`rail.projects.file_template.RailProjectFileTemplate` defines a template that can be used to construct file names using a `path_template` and a set of interpolants.

For example the path_template might be `a_file/{flavor}_data.hdf5`
and the interpolants would be `['flavor']`

When called with a dict of interpolants such as `{flavor: 'baseline}`
path_template would get expanded out to `a_file/baseline_data.hdf5`.
    


CatalogInstance
---------------

:py:class:`rail.projects.catalog_template.RailProjectCatalogInstance` holds information need to make a coherent catalog
of files using a `path_template` and `iteration_vars` to fill in the interpolation in the file name.

For example the `path_template` might be `a_file/{healpix}/data.parqut`
and the `interation_vars` would be `['healpix']`.

When called with a dict such as `{healpix : [3433, 3344]}` the
path_template would get expanded out to two files:

`a_file/3433/data.parqut`
`a_file/3344/data.parqut`


CatalogTemplate
---------------

:py:class:`rail.projects.catalog_template.RailProjectCatalogTemplate` holds a template for a catalog associated with a project

The makes a coherent catalog of files using a `path_template` interpolants and `iteration_vars` 
fill in the interpolation in the file name.

For example the `path_template` might be `a_file/{healpix}/{flavor}_data.hdf5`
and the interpolants would be `['flavor']` and `interation_vars` would be `['healpix']`.

When called with a dict such as `{flavor: baseline, healpix : [3433, 3344]}` the
`path_template` would get expanded out to two files:

`a_file/3433/baseline_data.hdf5`
`a_file/3344/baseline_data.hdf5`


=====================
Algorithm definitions
=====================

:py:class:`rail.projects.algorithm_holder.RailAlgorithmHolder` holds information about the python classes that implement a particular algorithm

This has the information needed to create the associated classes, namely the name of the python module in which they live, and the
names of the classes themselves.


There are several sub-classes of `RailAlgorithmHolder` for different types of algorithms.


PZAlgorithm
-----------

:py:class:`rail.projects.algorithm_holder.RailPZAlgorithmHolder` for algorithms that estimate per-object p(z).
This wraps both the `Inform` and `Estimate` classes.

The `Inform` class will typically be a `CatInformer` type `RailStage`, used to train the model for p(z) estimation.

The `Estimate` class will typically be a `CatEstimator` type `RailStage`, which uses the trained model for p(z) estimation.

A set of `PZAlgorithm` are used as inputs to several of the pipelines, specifying that the set of algorithms to run the pipeline with,


Summarizer
----------

:py:class:`rail.projects.algorithm_holder.RailSummarizerAlgorithmHolder` for algorithms that make ensemble n(z) from a set of p(z).

This wraps the `Summarize` class, which is typically a `PZSummarizer` type `RailStage`.

A set of `Summarizer` are used as inputs to the tomography-related pipelines, specifying that the set of algorithms to obtain n(z) information.


Classifier
----------

:py:class:`rail.projects.algorithm_holder.RailClassificationAlgorithmHolder` for algorithms that assign objects to tomographic bins.

This wraps the `Classify` class, which is typically a `PZSummarizer` type `RailStage`.

A set of `Classifier` are used as inputs to the tomography-related pipelines, specifying that the set of algorithms to assign objects to tomographic bins.


SpecSelection
-------------

:py:class:`rail.projects.algorithm_holder.RailSpecSelectionAlgorithmHolder` for algorithms that emulate spectrosopic selections.

This wraps the `SpecSelection` class, which is typically a `PZSummarizer` type `RailStage`.

A set of `SpecSelection` are used as inputs to the observation emulation pipelines, specifying that the set of algorithms to emulate spectrosopic selections.


ErrorModel
----------

:py:class:`rail.projects.algorithm_holder.RailErrorModelAlgorithmHolder` for algorithms that emulate photometric errors.

This wraps the `ErrorModel` class, which is typically a `ErrorModel` type `RailStage`

A set of `ErrorModel` are used as inputs to the observation emulation pipelines, specifying that the set of algorithms to emulate photometric errors.


Reducer
-------

:py:class:`rail.projects.algorithm_holder.RailReducerAlgorithmHolder` for algorithms that reduce data sets by applying selections and removing unneed columns.

This wraps the `Reduce` class, which is typically a `RailReducer` object.

Typically a single `Reducer` is used to prepare data for a particular project, possible apply a few different selections along the way.


Subsampler
----------

:py:class:`rail.projects.algorithm_holder.RailSubsamplerAlgorithmHolder` for algorithms that sumsample catalogs to provide testing and training data sets.

This wraps the `Subsample` class, which is typically a `RailSubsampler` object.

Typically a single `Subsample` is used to create a number of different test and training data sets for a particular project.



Algorithm configurations
========================

Selection
---------

:py:class:`rail.projects.selection_factor.RailSelection` just provides parameters such as the selection cuts needed by reducers.


Subsample
---------

:py:class:`rail.projects.subsample_factor.RailSubsample` just provides parameters such as the random number seed and number of object requested need by subsamplers.


================
Plot definitions
================


Plotter
-------

:py:class:`rail.plotting.plotter.RailPlotter` and its subsclasses make different types of plots.

The `class_name` parameter in the yaml file specifies which sub-class to use, and the other parameters specify things like the axes ranges, binning, etc...



PlotterList
-----------

:py:class:`rail.plotting.plotter.RailPlotterList` collects a set of plotter that can all run on the same data.  E.g., plotters that can all run on
a dict that looks like `{truth:np.ndarray, pointEstimates: np.ndarray}` could be put into a `PlotterList`.  This make it easier to collect similar
types of plots.



===========================
Plotting dataset defintions
===========================


Dataset
-------

:py:class:`rail.plotting.dataset_holder.RailDatasetHolder` and its subsclasses make different types of datasets.

The `class_name` parameter in the yaml file specifies which sub-class to use, and the other parameters specify the keys needed to specify a unique dataset.


DatasetList
-----------

:py:class:`rail.plotting. dataset_holder.RailDatasetListHolder` collects a set of matching dataset, i.e., that contain the same structure of data, such as
a dict that looks like `{truth:np.ndarray, pointEstimates: np.ndarray}`.


Project
-------

:py:class:`rail.plotting. dataset_holder.RailProjectHolder` wraps a particular project, so that it can be used by the `Dataset` components.



======================
Plot Group definitions
======================


PlotGroup
---------

:py:class:`rail.plotting.plot_group.RailPlotGroup` defines a set of plots to make by iterating over a `PlotterList` and a `DatasetList`.



*********
Factories
*********
    

==============
Factory basics
==============

A Factory is a python class that can make specific type or types of
components, assign names to each, and keep track of what it has made.

The basic interface to Factories is the :py:class:`rail.projects.factory_mixin.FactoryMixin` class, which defines a few things,

1. The "Factory pattern" of having a singleton instance of the factory that manages all the components of particular types, and class methods to interact with the instance.
2. A `client_classes` class member object specifying what types of components a particular factory manages.
3. Methods to add objects to a factory, and reset the factory contents.
4. Interfaces for reading and writing objects to and from yaml files.
5. Type validation, to ensure that only the correct types of objects are created or added to factories.


==================
Specific Factories
==================

.. list-table:: Factories
   :widths: 40 10 10 40
   :header-rows: 1

   * - Factory Class
     - Yaml Tag
     - Example Yaml File
     - Managed Classes
   * - :py:class:`rail.projects.project_file_factory.RailProjectFileFactory`
     - `Files`
     - `tests/ci_project_files.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_project_files.yaml>`_
     - `RailProjectFileInstance`, `RailProjectFileTemplate`
   * - :py:class:`rail.projects.catalog_factory.RailCatalogFactory`
     - `Catalogs`
     - `tests/ci_catalogs.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_catalogs.yaml>`_       
     - `RailProjectCatalogInstance`, `RailProjectCatalogTemplate`
   * - :py:class:`rail.projects.subsample_factory.RailSubsampleFactory`
     - `Subsamples`
     - `tests/ci_subsamples.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_subsamples.yaml>`_       
     - `RailSubsample`
   * - :py:class:`rail.projects.selection_factory.RailSelectionFactory`
     - `Selections`
     - `tests/ci_selections.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_selections.yaml>`_
     - `RailSelection`
   * - :py:class:`rail.projects.algorithm_factory.RailAlgorithmFactory`
     - `PZAlgorithms`
     - `tests/ci_algorithms.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_algorithms.yaml>`_
     - `RailPZAlgorithmHolder`
   * - 
     - `Classifiers`
     -
     - `RailClassificationAlgorithmHolder`
   * - 
     - `Summarizers`
     -
     - `RailSummarizerAlgorithmHolder`
   * - 
     - `SpecSelections`
     -
     - `RailSpecSelectionAlgorithmHolder`
   * - 
     - `ErrorModels`
     -
     - `RailErrorModelAlgorithmHolder`
   * - 
     - `Subsamplers`
     -
     - `RailSubsamplerAlgorithmHolder`
   * - 
     - `Reducers`
     -
     - `RailReducerAlgorithmHolder`
   * - :py:class:`rail.projects.pipeline_factory.RailPipelineFactory`
     - `Pipelines`
     - `tests/ci_pipelines.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_pipelines.yaml>`_
     - `RailPipelineTemplate`, `RailPipelineInstance`
   * - :py:class:`rail.plotting.plotter_factory.RailPlotterFactory`
     - `Plots`
     - `tests/ci_plots.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_plots.yaml>`_
     - `RailPlotter`, `RailPlotterList`
   * - :py:class:`rail.plotting.dataset_factory.RailDatasetFactory`
     - `Data`
     - `tests/ci_datasets.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_datasets.yaml>`_
     - `RailDatasetHolder`, `RailDatasetListHolder`, `RailProjectHolder`
   * - :py:class:`rail.plotting.plot_group_factory.RailPlotGroupFactory`
     - `PlotGroups`
     - `tests/ci_plot_groups.yaml <https://github.com/LSSTDESC/rail_projects/blob/main/tests/ci_plot_groups.yaml>`_
     - `RailPlotGroup`

