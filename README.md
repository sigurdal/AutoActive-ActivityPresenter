# AutoActive Research Environment
Last updated: 2021-May-18

SINTEF - https://www.sintef.com

## License
Apache License Version 2.0

## Installation
ActivityPresenter is available as a binary package for Microsoft Windows in Microsoft Store at the following link:
https://www.microsoft.com/en-us/p/activity-presenter/9n01v94ljlx7?

You can also clone the repository and build it localy, however, you will need a self signed certificate. If you do not have a self signed certificate you can otain one by following these instructions: https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-self-signed-certificate 

To add the certificate to the project 
1. Open the solution in Visual Studio
2. Right click on ActivityPresenter.UWP (Universal Windows) in the solution explorer and choose properties 
3. Choose Package Manifest in the Application tab
4. Choose the Packaging tab 
5. Click on Choose Certificate and select from file

You should now be able to compile and run the project.

The ActivityPresenter software is implemented using Xamarin and Xamarin.Forms. A Xamarin app runs on various platforms through bindings to a native runtime component. For Xamarin, this runtime is a Common Language Runtime (CLR), which was created for Windows, but is also supported on Linux, Android and iOS through the Mono project. Currently, ARE is tested on Microsoft Windows. However, future porting to Android is planned. Data between the various application parts is shared through a common internal data bus providing a flexible architecture.

## Community guidelines
All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome. If you find any bugs, have any ideas for improving the software or need support please create a new issue.

## Binary distribution
AutoActive Research Environment (ARE) is developed in the research project AutoActive (Norwegian Research Council project, project number 282039) to enable easy handling (synchronization, visualization, analysation) of sensor data from different commercially available wearable sensors for human activity and video.
ARE supports the following operations:

1. Import of 
   - sensor data from commercial sensors like IMU sensors
   - timeseries data like ultrasound time-of-flight (TOF) data
   - processed data like gesture detection classification
   - comma separated values (.csv), excel tables (.xlsx)
   - video
2. Synchronization (in time) between 
   - video and sensor data
   - various sensor data
3. Visualisation and inspection of multiple sensor data and video
4. Support for analysis tools like MATLAB® and Python
5. Organized storage of data from multiple sources

## Why?
The most time consuming, and typically least enjoyable task for data scientists is collecting, cleaning, and organizing data (Source: CrowdFlowerDataScientist report 2017 ). This is caused by a tool-gap, as there are few established good tools available for this work. ARE is intended to ease this by providing a tool to simplify this work.

## How?
The target platform is an easy-to-use toolbox that provides playback of video together with sensor data, data storage and replay for offline simulations, generic components and software interfaces for processing and visualization of data streams. The approach is based on IoT state-of-the-art platforms and software frameworks. It provides the tools for the design, evaluation and tuning of fusion and processing algorithms for multi-sensory systems. The platform also supports rapid prototyping of demonstrators and experimental setups.

## Architecture description
ARE consists of the following three main parts:
-	Application –ActivityPresenter, main GUI for ARE
-	File format – organization of data into session archives
-	Interfacing of other tools- data analysis, data import and data export

## Application, ActivityPresenter
The ActivityPresenter is the main visualization tool in ARE.
Activity presenter provides the following functionality:
-	File import
Video and time series data can be automatically imported into the tool. CSV, Excel files, as well as binary IMU data can be imported automatically. The import function is plugin-based and can easily be extended for new types of data. The tool handles data import timing automatically, hence timing information in various format is automatically converted to a common time format.
-	Data visualisation
Video and figures from data from arbitrary sensors can be placed and resized in the main window. Data can be stepped through in a frame-by-frame manner or played back in real time, slow-motion or at a fast-forward speed. The visualisation point can be changed in the data-track line. The figure views include autoscaling of the plots, visualization of the current time, legends describing each line, dynamic values on the axes, independent scaling of lines in each figure, and an option to freeze the current scale to prevent rescaling due to outliers. The plotting style can be manually changed between line plots (default), scatter plots or column plots. Additionally, the length of the time axis can be changed to decide how much of the time series is seen in the plot window. 
-	Open archive and save
Data is organized and stored in archives that can be created and opened from ActivityPresenter. The archives can also be created in MATLAB® or Python by using the developed toolboxes. See File Format section for more information.
-	Synchronize
Video and data from arbitrary sensors can be synchronized in time by placing synchronization points in each dataset. The synchronization is performed by using one dataset as master, and then moving the other datasets (slaves) relative to the master. 
-	Head2Head
The head-to-head module is intended to compare two separate data recording sessions performed on different occasions or by different users. Two sets of data can be shown and the point in time can be changed individually to compare the two different sessions. 

## File Format - Sessions
The concept of sessions is key to how data is stored in ARE; they are the root containers of datasets. A session represents an activity – bounded in time and space – performed by one user of the platform and stores the information about the context of the activity, and the data generated during that activity. Examples of such activities include a data recording in a lab or in the field, data processing using MATLAB, and comparing two datasets from two different recordings using the ActivityPresenter App.
Sessions have two important features to help with traceability and reproducibility:
1.	When a session is saved (written to persistent storage), it becomes immutable, meaning that neither the metadata nor data stored inside it can be changed. The different parts of the platform should ensure that this property is met as far as possible.
2.	Sessions can be based on previous sessions, for the purpose of declaring that data in those previous sessions were used to generate the data in the current session. Together with the immutability, this enables traceability and reproducibility without having to have multiple copies of possibly large datasets. 
For programmers, sessions are quite analogous to commits in a version-control system, enabling control over source code versions without copying the whole source code for every version.
Sessions are identified by a unique identifier compliant with RFC4122 version 4, commonly known as a random UUID or GUID.

File format for data storage (Archive)
The following requirements were made for the file format:
a)	Storing multiple datasets in a single file
b)	Storing metadata necessary to describe the data itself, its origin, and structure of coupled datasets
c)	Compressing data in a binary format suitable for different types of data (tables, video, images, etc.) 

None of the conventional data storage file formats supported our requirements, hence a custom file format based on the ZIP archive file format was introduced. This format has libraries and tools for practically all used platforms and is widely adopted. The custom file format has the following properties:
1.	Stores meta-data in JSON-encoded files. This format is both easily encoded and decoded in software, as well as somewhat readable for humans.
2.	The structure of the data (organization) is stored in the JSON files, as well as kept in the directory structure within the ZIP archive.
3.	Data is stored using suitable file formats for each kind of data, preferably with some binary compression. E.g. tabular data is stored in Parquet-files, videos as .mp4 files, and images as .jpg.
4.	The ZIP archive is itself written using uncompressed/stored mode. This way, individual files inside the archive can easily be read without decompressing. Additional ZIP compression schemes would not result in much gain in terms of file size for data that has already been compressed.
By using this custom file format, multiple datasets can be stored in a single file, which makes it easy to transfer between devices and users. Relying on different kinds of storage and compression for different types of data, the file size will be kept minimal. Also, since there are many tools for working with ZIP archives, users can easily extract data manually by unzipping the archive locally, and other software should be able to extract data without relying on the ARE tools.

## Interfacing of other tools
**MATLAB library**

The session lib is a set of classes using ArchiveWriter and ArchiveReader to write and read ARE files (.aaz files). The library is based on a set of classes storing data of different formats all based on the class Dataobject as a super class. The class Dataobject supports all transformations needed for converting between MATLAB formats and the archive storage formats. The split into user- and meta-data is built into the class with accessors available for read/write of user-data. This makes its behaviour to be like a Struct element when using it in a MATLAB script. 
All Dataobject sub-classes are plugins, each identified by a jsonType string. This makes it possible to load a session from json and create objects representing its content. The plugin-register acts as an object factory, finding and creating the correct jsonPlugin object on demand. It is possible for the user to add custom plugins without adding them to the library. When saving a session to archive, it is possible to transform native MATLAB types into a plugin making transformation for storage possible. This is done for MATLAB tables. The plugin is identified by its classType. The plugin register act as an object factory finding and creating the correct native Plugin object on demand
The MATLAB interface is implemented as a MATLAB toolbox that can easily be deployed and includes documentation with examples.
Available here: https://github.com/SINTEF/AutoActive-Matlab-toolbox

**Python library**

A Python library is available and supports similar functionality as the MATLAB library.
Available here: https://github.com/SINTEF/AutoActive-Python-toolbox


## Instruction videos for ActivityPresenter
* [Import data](https://user-images.githubusercontent.com/2269482/115543050-3fdfd480-a2a1-11eb-8c5d-1150adb3e2b1.mp4)
* [Syncronize data](https://user-images.githubusercontent.com/2269482/115603893-ff07b000-a2e0-11eb-8327-e0b5244880c8.mp4)
* [Save data](https://user-images.githubusercontent.com/2269482/115671110-758cc800-a34a-11eb-86a7-d1c8d9439a22.mp4)
* [Visualize data](https://user-images.githubusercontent.com/2269482/115603974-1ba3e800-a2e1-11eb-9660-a314641c0cd6.mp4)
* [Matlab toolbox](https://user-images.githubusercontent.com/2269482/115671205-8dfce280-a34a-11eb-8892-1031a8101da4.mp4)
* [Multiple aaz archives](https://user-images.githubusercontent.com/2269482/115604046-324a3f00-a2e1-11eb-8253-8d73e52fba69.mp4)
* [Automatic synchronization of data](https://user-images.githubusercontent.com/2269482/115604090-4130f180-a2e1-11eb-9505-2d79537ac827.mp4)
* [Head to head](https://user-images.githubusercontent.com/2269482/115604146-5443c180-a2e1-11eb-802a-2e11029f6781.mp4)
* [Save and load views](https://user-images.githubusercontent.com/2269482/115604181-5dcd2980-a2e1-11eb-8cb7-bb1b40573ea9.mp4)
