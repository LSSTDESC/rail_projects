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

.. autoclass:: rail.projects.file_template.RailProjectFileInstance
    :noindex:

       

FileTemplate
------------

.. autoclass:: rail.projects.file_template.RailProjectFileTemplate
    :noindex:

    
CatalogInstance
---------------

.. autoclass:: rail.projects.catalog_template.RailProjectCatalogInstance
    :noindex:



CatalogTemplate
---------------

.. autoclass:: rail.projects.catalog_template.RailProjectCatalogInstance
    :noindex:


=====================
Algorithm definitions
=====================

.. autoclass:: rail.projects.algorithm_holder.RailAlgorithmHolder
    :noindex:

There are several sub-classes of `RailAlgorithmHolder` for different types of algorithms.


PZAlgorithm
-----------

.. autoclass:: rail.projects.algorithm_holder.RailPZAlgorithmHolder
    :noindex:

       
Summarizer
----------

.. autoclass:: rail.projects.algorithm_holder.RailSummarizerAlgorithmHolder
    :noindex:


Classifier
----------

.. autoclass:: rail.projects.algorithm_holder.RailClassificationAlgorithmHolder
    :noindex:


SpecSelection
-------------

.. autoclass:: rail.projects.algorithm_holder.RailSpecSelectionAlgorithmHolder
    :noindex:


ErrorModel
----------

.. autoclass:: rail.projects.algorithm_holder.RailErrorModelAlgorithmHolder
    :noindex:


Reducer
-------

.. autoclass:: rail.projects.algorithm_holder.RailReducerAlgorithmHolder
    :noindex:


Subsampler
----------

.. autoclass:: rail.projects.algorithm_holder.RailSubsamplerAlgorithmHolder
    :noindex:



Algorithm configurations
========================

Selection
---------

.. autoclass:: rail.projects.selection_factory.RailSelection
    :noindex:


Subsample
---------

.. autoclass:: rail.projects.subsample_factory.RailSubsample
    :noindex:


================
Plot definitions
================


Plotter
-------

.. autoclass:: rail.plotting.plotter.RailPlotter
    :noindex:
    :no-members:


PlotterList
-----------

.. autoclass:: rail.plotting.plotter.RailPlotterList
    :noindex:


===========================
Plotting dataset defintions
===========================


Dataset
-------

.. autoclass:: rail.plotting.dataset_holder.RailDatasetHolder
    :noindex:
    :no-members:

The `class_name` parameter in the yaml file specifies which sub-class to use, and the other parameters specify the keys needed to specify a unique dataset.


DatasetList
-----------

.. autoclass:: rail.plotting.dataset_holder.RailDatasetListHolder
    :noindex:


Project
-------

.. autoclass:: rail.plotting.dataset_holder.RailProjectHolder
    :noindex:


======================
Plot Group definitions
======================


PlotGroup
---------

.. autoclass:: rail.plotting.plot_group.RailPlotGroup
    :noindex:
