*********
Factories
*********
    

==============
Factory basics
==============

.. autoclass:: rail.projects.factory_mixin.RailFactoryMixin
    :noindex:
    :no-members:


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



.. autoclass:: rail.projects.algorithm_factory.RailAlgorithmFactory
    :noindex:

.. autoclass:: rail.projects.catalog_factory.RailCatalogFactory
    :noindex:

.. autoclass:: rail.projects.pipeline_factory.RailPipelineFactory
    :noindex:

.. autoclass:: rail.projects.project_file_factory.RailProjectFileFactory
    :noindex:

.. autoclass:: rail.projects.selection_factory.RailSelectionFactory
    :noindex:

.. autoclass:: rail.projects.subsample_factory.RailSubsampleFactory
    :noindex:

.. autoclass:: rail.plotting.plotter_factory.RailPlotterFactory
    :noindex:

.. autoclass:: rail.plotting.dataset_factory.RailDatasetFactory
    :noindex:

.. autoclass:: rail.plotting.plot_group_factory.RailPlotGroupFactory
    :noindex:

