=============================
Adding a new RailDataset type
=============================

Because of the variety of formats of files in RAIL, and the variety of analysis flavors
in a ``RailProject``, it is useful to be able to define the particular types of
datasets that are needed to make specific plots. These are implemented as subclasses of the :py:class:`rail.plotting.dataset.RailDataset` class.
A ``RailDataset`` is intended define the quantities needed to make a particular type of plot.


New RailDataset Example
-----------------------

The following example has all of the required pieces of a ``RailDataset`` and almost nothing else.

.. code-block:: python

    class RailPZPointEstimateDataset(RailDataset):
        """Dataet to hold a vector p(z) point estimates and corresponding
        true redshifts
        """

        data_types = dict(
            truth=np.ndarray,
            pointEstimate=np.ndarray,
        )


The required pieces, in the order that they appear are:

#. The ``RailPZPointEstimateDataset (RailDataset):`` defines a class called ``RailPZPointEstimateDataset`` and specifies that it inherits from ``RailDataset``.

#. The ``data_types`` define names and expected data types of the required data.
