# data-pipelines
Energy system data pipelines that can populate model instances in the ines format. The intent is to have high resolution datasets from which user can choose a model instance with the resolutions that are applicable for the case study at hand.

The repository should not hold data itself, just scripts that can fetch and process data. Data can be stored e.g. in Zenodo when there is no reliable long term url for the data. The folder structure is fixed - one folder for global data pipelines, a folder for each continent and then sub-folders for countries or other regions within the continent. Hopefully most data can be fetched at the highest level data is available.
