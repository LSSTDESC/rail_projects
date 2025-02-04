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

