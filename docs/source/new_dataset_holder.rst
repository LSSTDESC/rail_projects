==============================
Adding a new RailDatasetHolder
==============================

Because of the variety of formats of files in RAIL, and the variety of analysis flavors
in a ``RailProject``, it is useful to be able to have re-usable tools that wrap particular types
datasets from a ``RailProject`` These are implemented as subclasses of the :py:class:`rail.plotting.dataset_holder.RailDatasetHolder` class.
A ``RailDatasetHolder`` is intended to take a particular set of inputs and
extract a particular set of data from the ``RailProject``.  The inputs and outputs
are all defined in particular ways to allow ``RailDatasetHolder``
objects to be integrated into larger data analysis pipelines.


New RailDatasetHolder Example
-----------------------------

The following example has all of the required pieces of a ``RailDatasetHolder`` and almost nothing else.

.. code-block:: python

    class RailPZPointEstimateDataHolder(RailDatasetHolder):
        """Simple class for holding a dataset for plotting data that comes from a RailProject"""

        config_options: dict[str, StageParameter] = dict(
            name=StageParameter(str, None, fmt="%s", required=True, msg="Dataset name"),
            project=StageParameter(
                str, None, fmt="%s", required=True, msg="RailProject name"
            ),
            selection=StageParameter(
                str, None, fmt="%s", required=True, msg="RailProject data selection"
            ),
            flavor=StageParameter(
                str, None, fmt="%s", required=True, msg="RailProject analysis flavor"
            ),
            tag=StageParameter(
                str, None, fmt="%s", required=True, msg="RailProject file tag"
            ),
            algo=StageParameter(
                str, None, fmt="%s", required=True, msg="RailProject algorithm"
            ),
        )

        extractor_inputs: dict = {
            "project": RailProject,
            "selection": str,
            "flavor": str,
            "tag": str,
            "algo": str,
        }

	output_type: type[RailDataset] = RailPZPointEstimateDataset

        def __init__(self, **kwargs: Any):
            RailDatasetHolder.__init__(self, **kwargs)
            self._project: RailProject | None = None

        def __repr__(self) -> str:
            ret_str = (
                f"{self.config.extractor} "
                "( "
                f"{self.config.project}, "
                f"{self.config.selection}_{self.config.flavor}_{self.config.tag}_{self.config.algo}"
                ")"
            )
            return ret_str

        def get_extractor_inputs(self) -> dict[str, Any]:
            if self._project is None:
                self._project = RailDatasetFactory.get_project(self.config.project).resolve()
            the_extractor_inputs = dict(
                project=self._project,
                selection=self.config.selection,
                flavor=self.config.flavor,
                tag=self.config.tag,
                algo=self.config.algo,
            )
            self._validate_extractor_inputs(**the_extractor_inputs)
            return the_extractor_inputs

	def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
            return get_pz_point_estimate_data(**kwargs)
	    
        @classmethod
        def generate_dataset_dict(
            cls,
            **kwargs: Any,
        ) -> list[dict[str, Any]]:


The required pieces, in the order that they appear are:

#. The ``RailProjectDatasetHolder(RailDatasetHolder):`` defines a class called ``RailProjectDatasetHolder`` and specifies that it inherits from ``RailDatasetHolder``.

#. The ``config_options`` lines define the configuration parameters for this class, as well as their default values.  Note that we are specifying a helper class to actually extract the data.

#. The ``extractor_inputs = [('input', PqHandle)]`` and ``outputs = [('output', PqHandle)]``  define the inputs that will be based to the 

#. The ``output_type: type[RailDataset] = RailPZPointEstimateDataset``
   line specifies that this class will return a
   RailPZPointEstimateDataset dataset.
   
#. The ``__init__`` method does any class-specific initialization, in this case defining that this class will store and project and extractor 

#. The ``__repr__`` method is optional, here it gives a useful representation of the class

#. The ``get_extractor_inputs()`` method does the first part of the actual work, note
   that it doesn't take any arguments, that it uses the factories to
   find the helper objects and passes algo it's configuration and
   validates it's outputs

#. The ``_get_data()`` method does the rest of actual work (in this case it passes it off to a utility function ``get_pz_point_estimate_data`` which knows how to extract data from the ``RailProject``
   
#. The ``generate_dataset_dict()`` can scan a ``RailProject`` and generate a dictionary of all the available datasets
