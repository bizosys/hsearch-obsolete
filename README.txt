See the docs directory or http://www.bizosys.com/hsearch/api/

Quick Start (MSI Installation)
----------------------
This will index 50000 location information using sample data file and search for a keyword named somerset in it.

1. Install the MSI.
2. From the Windows Start Menu, Go to HSearch and then click on "Step-1 Start". This will create an tray icon at right bottom part of your windows with letter H. 
3. Right click on the icon and click "Start HBase". HBase will be started and a message on console will come (HBase Server started)
4. From the Windows Start Menu, Go to HSearch and then click on "Step - 2 Index Sample Data"
5. From the Windows Start Menu, Go to HSearch and then click on "Step - 3 Search Sample Data"



Quick Start (TAR FILE)
----------------------
This will index 50000 location information using sample data file and search for a keyword named somerset in it.

1. Create a folder "c:\bizosys" and copy "hsearch-0.90.tar" file into this folder.
2. Extract the tar file here. It will create c:\bizosys\hsearch-0.90 folder.
3. Open a command line prompt.  Type
    cd C:\bizosys\hsearch-0.90\bin
4. Start hsearch tray icon typing command. This will create an icon at right bottom part of your windows desktop shortcut tray. 
   hsearch.bat
5. Right click on the icon and click "Start HBase". HBase will be started and a message on console will come (HBase Server started)
6. Open another window and Goto benchmark folder
   cd C:\bizosys\hsearch-0.90\benchmark
7. Start indexing freebase location databases typing command, 
   freebaseW.bat
8. Once indexed, Look for keyword "somerset" typing command, 
   query.bat