************************************
Components, Factories, and Libraries
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
