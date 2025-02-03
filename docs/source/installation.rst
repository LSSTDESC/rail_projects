************
Installation
************


RAIL is actually distributed as several software packages.

Some of the RAIL algorithms have dependencies that are sensitive to out-of-date code versions, therefore it is strongly recommended that you create a new dedicated virtual environment for RAIL to avoid problems with pip/conda failing to update some packages that you have previously installed during installation of RAIL.  Also, having multiple version of RAIL in your path can cause difficult to diagnose problems, so we encourage you to make sure that you don't have an existing version of RAIL installed in your `.local` area or in your base conda environment.


There are two ways you might choose to install `rail_projects`

1. `Production Installation`_: Just install `rail_projects` in an
   existing an existing conda environment using pip.
2. `Exploration Installation`_: Download the `rail_projects` source
   code and example notebooks, and install from the local version using pip.
3. `Developer Installation`_: Download the `rail_projects` source
   code and example notebooks and install from the local version using
   pip in "editable" mode.


In call cases we recommend you first install RAIL by following the either the "Production
Installation" (if you want all of the RAIL "ecosystem") or "Algorithm
Installation" (if you only want access to a sub-set of the RAIL
algorithms) instructions 
`on the RAIL installation page <https://rail-hub.readthedocs.io/en/latest/source/installation.html>`_


=======================
Production Installation
=======================   

Here we will be installing ``rail_projects`` into an existing conda environment "[env]".

.. code-block:: bash

    conda activate [env]
    pip install pz-rail-projects	


========================   
Exploration Installation
========================

Here we will be installing the source code from `rail
<https://github.com/LSSTDESC/rail_projects>`_ to access all of the
demonstration notebooks.


.. code-block:: bash

    conda activate [env]
    git clone https://github.com/LSSTDESC/rail_projects.git
    cd rail_projects
    pip install .[dev]


At that point you should be able to run the demonstration notebooks, e.g.;

.. code-block:: bash

    jupyter-notebook examples
	  

======================	  
Developer Installation
======================   

Here we will be installing the source code from `rail
<https://github.com/LSSTDESC/rail_projects>`_ to be able to develop
the source code.


.. tabs::

   .. group-tab:: General

      .. code-block:: bash

	  conda activate [env]
          git clone https://github.com/LSSTDESC/rail_projects.git
          cd rail_projects
          pip install -e .[dev]


   .. group-tab:: zsh (e.g., Mac M1+ default)

      .. code-block:: bash

	  conda activate [env]	      
          git clone https://github.com/LSSTDESC/rail_projects.git
          cd rail_projects
          pip install -e '.[dev]'

    
=============
RAIL packages
=============

Depending on how you want to use RAIL you will be installing one or
more `RAIL packages <https://rail-hub.readthedocs.io/en/latest/source/installation.html#rail-packages>`_


=============================
Adding your kernel to jupyter
=============================
If you want to use the kernel that you have just created to run RAIL example demos, then you may need to explicitly add an ipython kernel.  You may need to first install ipykernel with `conda install ipykernel`.  You can do then add your kernel with the following command, making sure that you have the conda environment that you wish to add activated.  From your environment, execute the command:
`python -m ipykernel install --user --name [nametocallnewkernel]`
(you may or may not need to prepend `sudo` depending on your permissions).  When you next start up Jupyter you should see a kernel with your new name as an option, including using the Jupyter interface at NERSC.

