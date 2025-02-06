========================
Adding a new RailPlotter
========================

All of the various plotting classes 
are implemented as subclasses of the :py:class:`rail.plotting.plotter.RailPlotter` class.
A ``RailPlotter`` is intended to take a particular set of inputs and configuration parameters, 
run a single bit of analysis, and produce one or more plots.  The inputs, outputs
and configuration parameters are all defined in particular ways to allow ``RailPlotter``
objects to be integrated into larger data analysis pipelines.


New RailPlotter Example
-----------------------

The following example has all of the required pieces of a ``RailPlotter`` and almost nothing else.

.. code-block:: python

    class PZPlotterPointEstimateVsTrueHist2D(RailPlotter):
        """Class to make a 2D histogram of p(z) point estimates
        versus true redshift
        """

        config_options: dict[str, StageParameter] = RailPlotter.config_options.copy()
        config_options.update(
            z_min=StageParameter(float, 0.0, fmt="%0.2f", msg="Minimum Redshift"),
            z_max=StageParameter(float, 3.0, fmt="%0.2f", msg="Maximum Redshift"),
            n_zbins=StageParameter(int, 150, fmt="%i", msg="Number of z bins"),
        )

        input_type = RailPZPointEstimateDataset

        def _make_2d_hist_plot(
            self,
            prefix: str,
            truth: np.ndarray,
            pointEstimate: np.ndarray,
            dataset_holder: RailDatasetHolder | None = None,
        ) -> RailPlotHolder:
            figure, axes = plt.subplots()
            bin_edges = np.linspace(
                self.config.z_min, self.config.z_max, self.config.n_zbins + 1
            )
            axes.hist2d(
                truth,
                pointEstimate,
                bins=(bin_edges, bin_edges),
            )
            plt.xlabel("True Redshift")
            plt.ylabel("Estimated Redshift")
            plot_name = self._make_full_plot_name(prefix, "")
            return RailPlotHolder(
                name=plot_name, figure=figure, plotter=self, dataset_holder=dataset_holder
            )

        def _make_plots(self, prefix: str, **kwargs: Any) -> dict[str, RailPlotHolder]:
            find_only = kwargs.get("find_only", False)
            figtype = kwargs.get("figtype", "png")
            dataset_holder = kwargs.get("dataset_holder")
            out_dict: dict[str, RailPlotHolder] = {}
            truth: np.ndarray = kwargs["truth"]
            pointEstimate: np.ndarray = kwargs["pointEstimate"]
            if find_only:
                plot_name = self._make_full_plot_name(prefix, "")
                assert dataset_holder
                plot = RailPlotHolder(
                    name=plot_name,
                    path=os.path.join(dataset_holder.config.name, f"{plot_name}.{figtype}"),
                    plotter=self,
                    dataset_holder=dataset_holder,
                )
            else:
                plot = self._make_2d_hist_plot(
                    prefix=prefix,
                    truth=truth,
                    pointEstimate=pointEstimate,
                    dataset_holder=dataset_holder,
                )
            out_dict[plot.name] = plot
            return out_dict

      
The required pieces, in the order that they appear are:

#. The ``PZPlotterPointEstimateVsTrueHist2D(RailPlotter):`` defines a class called ``PZPlotterPointEstimateVsTrueHist2D`` and specifies that it inherits from ``RailPlotter``.

#. The ``config_options`` lines define the configuration parameters for this class, as well as their default values.  Note that here we are copying the configuration parameters from the ``RailPlotter`` as well as defining some new ones.

#. The ``input_type = RailPZPointEstimateDataset`` specifies that this
   plotter expects a :py:class:`rail.plotting.pz_plotters.RailPZPointEstimateDataset` type dataset, which in
   this case is an dict with one item (called ``truth``) that is a
   numpy array, and a second item (called ``pointEstimate``) that is a
   also a numpy array.

#. The ``__init__`` method does any class-specific initialization.  In this case there isn't any and the method is superfluous.

#. The ``_make_2d_hist_plot(...)`` method does the actual work, note it takes some of the same arguements are define in ``inputs`` and that it uses ``self.config`` to access the configuration parameters.

#. The ``_make_plots(self, prefix: str, **kwargs: Any)`` method provides an interface to format the data for _make_2d_hist_plot(), the arguments to this function are specified in the ``RailPlotter`` class
