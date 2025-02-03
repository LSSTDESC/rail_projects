==========================
Adding a new DataExtractor
==========================

Because of the variety of formats of files in RAIL, and the variety of analysis flavors
in a ``RailProject``, it is useful to be able to have re-usable tools that extract particular
datasets from a ``RailProject`` These are implemented as subclasses of the :py:class:`rail.plotting.data_extractor.RailProjectDataExtractor` class.
A ``RailProjectDataExtractor`` is intended to take a particular set of inputs and
extract a particular set of data from the ``RailProject``.  The inputs and outputs
are all defined in particular ways to allow ``RailProjectDataExtractor``
objects to be integrated into larger data analysis pipelines.


New DataExtractor Example
-------------------------

The following example has all of the required pieces of a ``RailProjectDataExtractor`` and almost nothing else.

.. code-block:: python

    class PZPointEstimateDataExtractor(RailProjectDataExtractor):
        """Class to extract true redshifts and one p(z) point estimate
        from a RailProject.

        This will return a dict:

        truth: np.ndarray
            True redshifts

        pointEstimate: np.ndarray
            Point estimates of the true redshifts
        """

        inputs: dict = {
            "project": RailProject,
            "selection": str,
            "flavor": str,
            "tag": str,
            "algo": str,
        }

        def _get_data(self, **kwargs: Any) -> dict[str, Any] | None:
            return get_pz_point_estimate_data(**kwargs)

        @classmethod
        def generate_dataset_dict(
            cls,
            **kwargs: Any,
        ) -> list[dict[str, Any]]:

      
The required pieces, in the order that they appear are:

#. The ``PZPointEstimateDataExtractor(RailProjectDataExtractor):`` defines a class called ``PZPointEstimateDataExtractor`` and specifies that it inherits from ``RailProjectDataExtractor``.

#. The ``inputs = [('input', PqHandle)]`` and ``outputs = [('output', PqHandle)]``  define the inputs, and the expected data types for those, in this case a ``RailProject`` and the keys needed to extract information from it

#. The ``_get_data()`` method does the actual work (in this case it passes it off to a utility function ``get_pz_point_estimate_data`` which knows how to extract data from the ``RailProject``

#. The ``generate_dataset_dict()`` can scan a ``RailProject`` and generate a dictionary of all the available datasets

