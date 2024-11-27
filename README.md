# data-pipelines
Energy system data pipelines that can populate model instances in the ines format. The intent is to have high resolution datasets from which user can build a model instance in geographical and temporal resolutions that are applicable for the case study at hand.

The repository should not hold data itself, just scripts that can fetch and process data. Data can be stored e.g. in Zenodo when there is no reliable long term url for the data. The folder structure is fixed - one folder for global data pipelines, a folder for each continent and then sub-folders for countries or other regions within the continent. Sector level data should be marked with an preceding underscore to keep them up in the alphabetic order. The folder name should also indicate the datasource, e.g. data-pipelines/europe/_industry-aidres. Hopefully most data can be fetched at the highest level data is available.


![European Flag](https://upload.wikimedia.org/wikipedia/commons/thumb/b/b7/Flag_of_Europe.svg/96px-Flag_of_Europe.svg.png)

*This project has received funding from the European Union's Horizon Programme under grant agreement No 101095998. 
The sole responsibility of this publication lies with the authors. The European Union is not responsible for any use that may be made of the information contained therein.
Every effort has been made to ensure complete and accurate information. 
However, the author(s) and members of the consortium cannot be held legally responsible for any mistake or fault information.*
