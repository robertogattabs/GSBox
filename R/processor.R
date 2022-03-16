#' a Processor
#'
#' @description  This is an implementation of the First Order Markov Model (firstOrderMarkovModel) for Process Mining issues.
#'                This class provides a minimal set of methods to handle with the FOMM model:
#'              The consturctor admit the following parameters:
#' parameters.list a list containing possible parameters to tune the model.
#' @param parameters.list a list containing the parameters. The possible ones are: 'considerAutoLoop' and 'threshold'. 'considerAutoLoop' is a boolean which indicates if the autoloops have to be admitted, while 'threshold' is the minimum value that a probability should have to do not be set to zero, in the transition matrix.
#' @import stringr XML zip
#' @export
#'
processor <- function( input_folder.dir = NA , output_folder.dir = NA ,  tmp_folder.dir=NA,
                       log_folder.dir=NA, cache_folder.dir=NA , sync_folder.dir=NA ) {

  global.lst.log_folder <- ""
  global.lst.tmp_folder <- ""
  global.lst.cache_folder <- ""
  global.sync.tmp_folder <- ""
  global.lst.input_folder <- ""
  global.lst.output_folder <- ""
  objLogHandler <- LogHandler()

  global.current.state <- "READY"

  start <- function() {

    set.current.state("RUNNING")

    while( get.current.state() =="RUNNING" ) {


      # -----------------------------------------------------------------------
      # POOLING della input_folder
      # -----------------------------------------------------------------------
      lst.res <- checkInputFolder( global.lst.input_folder$path , timeForSleepint = 2)

      # -----------------------------------------------------------------------
      # PARSING del TOKEN
      # -----------------------------------------------------------------------
      fileName <- lst.res$fileName
      fullFileName <- lst.res$fullFileName

      res <- openZippedPackage( fileName , fullFileName )

      # -----------------------------------------------------------------------
      # RUN dei servizi richiesti
      # -----------------------------------------------------------------------
      cleanUp.tmp.folders(temp = FALSE, run = TRUE)
      if( length(res$lst.run) != 1 ) {
        stop("ERROR: only one script per time, in the current version")
      }
      for( run.id in names(res$lst.run)) {
        # prendi lo script e guarda il tipo
        scriptAlias <- res$lst.run[[run.id]]$scriptAlias
        scriptType <- res$lst.script[[ scriptAlias ]]$scriptType
        scriptFileName <- res$lst.script[[ scriptAlias ]]$scriptFileName
        scriptFullFileName <- paste(c(global.lst.tmp_folder$path,"/",scriptFileName),collapse = '')
        executionFolderName <- paste(c("run.id_",run.id),collapse = '')
        executionFolderFullName <- paste(c(global.lst.tmp_folder$path,"/",executionFolderName),collapse = '')
        # check the presence of the expected files
        # browser()
        if( !file.exists(scriptFullFileName) ) stop("ERROR : the .R is missing")

        # ok, prepara l'env per eseguire lo script
        save.image(  paste(c(global.lst.tmp_folder$pathEnv,"/runningEnv.RData"),collapse = '')  )
        currentWD <- getwd()

        dir.create(executionFolderFullName)
        if( !dir.exists(executionFolderFullName) ) stop("ERROR : not able to build the executionFolder.. ")
        browser()

        setwd( global.lst.tmp_folder$path  )

        filettoneR <- readLines(scriptFileName)
        filettoneR <- str_replace_all(filettoneR,"##param.output.folderName##",executionFolderName)
        fileConn<-file(scriptFileName)
        writeLines(filettoneR, fileConn)
        close(fileConn)

        source(scriptFileName)

        setwd( currentWD  )
        browser()


      }


    }
  }

  checkInputFolder <- function( inputFolder , timeForSleepint = 2 ) {

    fileFound <- FALSE; whichFileName <- ""
    list.previous.files <- list()

    oldFileList <- c()
    while( fileFound == FALSE & get.current.state() =="RUNNING"  ) {
      arr.ff <- list.files( inputFolder )
      for(fileName in arr.ff) {
        fullFileName <- paste( c(inputFolder,"/",fileName), collapse = '')
        if( fileName %in% names(list.previous.files) ) {
          list.previous.files[[fileName]]$size == file.size( fullFileName )
          fileFound <- TRUE;  whichFileName <- fullFileName
        } else {
          # 'timeStamp' per ora non e' usato: in futuro potra' esserlo per evitare la starvation
          # dei job sottomessi (verranno privilegiati quelli piu' vecchi')
          list.previous.files[[fileName]] <- list( "size"=file.size( paste( fullFileName ) ),
                                                   "timeStamp" = format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
        }
      }
      Sys.sleep(timeForSleepint)
    }

    return( list( "fileName" = fileName,
                  "fullFileName" = whichFileName))

  }
  openZippedPackage <- function( fileName , fullFileName ) {

    tmpFolder <- global.lst.tmp_folder$path

    # vuota la tmpFolder
    # unlink(  paste(c( tmpFolder, "/*"),collapse = '') , recursive = TRUE)
    cleanUp.tmp.folders()

    # sposta il file e rimuovilo dalla inputFolder
    file.copy(from = fullFileName , to = tmpFolder)
    file.remove(fullFileName)

    # scompatta il file
    unzip(zipfile = paste( c(tmpFolder,"/",fileName), collapse = ''), exdir =  tmpFolder)
    file.remove(paste( c(tmpFolder,"/",fileName), collapse = ''))

    # carica il objXML e costruisci una struttura con i servizi richiesti (lst.servizio)
    # dopo aver letto l'XML

    objXML <- xmlInternalTreeParse(file = paste(c(tmpFolder,"/","description.xml"),collapse = ''))

    tokenInstanceUID <- unlist(xpathApply(objXML,path="/xml/XMLheader/tokenInstanceUID",xmlValue))
    runUID <- unlist(xpathApply(objXML,path="/xml/XMLheader/runUID",xmlValue))
    creationDateTime <- unlist(xpathApply(objXML,path="/xml/XMLheader/creationDateTime",xmlValue))

    # --------------------------------------------------
    # prima scorri le dataSources
    # --------------------------------------------------
    subXML <- xpathApply(objXML,path="/xml/XMLobj[@objType='dataSource']")
    lst.dataSource <- list()
    if( length(subXML) > 0 ) {
      for( ct in 1:length(subXML)) {

        subXMLDoc <- xmlDoc(subXML[[ct]])
        if( length(unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceType",xmlValue)))!=1 ) stop("ERROR : missing a 'dataSourceType' withing an XMLobj tag")
        if( length(unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceFileName",xmlValue)))!=1 ) stop("ERROR : missing a 'dataSourceFileName' withing an XMLobj tag")
        if( length(unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceAlias",xmlValue)))!=1 ) stop("ERROR : missing a 'dataSourceAlias' withing an XMLobj tag")
        if( unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceType",xmlValue))!="csv" ) stop("ERROR : only 'csv' is supported for the 'dataSourceType' tag, in the current version")

        dataSourceFileName <- unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceFileName",xmlValue))
        dataSourceAlias <- unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceAlias",xmlValue))
        dataSourceType <- unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceType",xmlValue))

        lst.dataSource[[dataSourceAlias]] <- list()
        lst.dataSource[[dataSourceAlias]]$dataSourceFileName <- dataSourceFileName
        lst.dataSource[[dataSourceAlias]]$dataSourceType <- dataSourceType

        free(subXMLDoc)
      }
    }

    # --------------------------------------------------
    # prima scorri le other
    # --------------------------------------------------
    subXML <- xpathApply(objXML,path="/xml/XMLobj[@objType='other']")
    lst.other <- list()
    if( length(subXML) > 0 ) {
      for( ct in 1:length(subXML)) {
       stop("ERROR : datatype 'other' Not yet supported ")
      }
    }

    # --------------------------------------------------
    # prima scorri gli Script
    # --------------------------------------------------
    subXML <- xpathApply(objXML,path="/xml/XMLobj[@objType='script']")
    lst.script <- list()
    if( length(subXML) > 0 ) {
      for( ct in 1:length(subXML)) {
        subXMLDoc <- xmlDoc(subXML[[ct]])
        scriptType <- unlist(xpathApply(subXMLDoc,path="/XMLobj/scriptType",xmlValue))
        scriptFileName <- unlist(xpathApply(subXMLDoc,path="/XMLobj/scriptFileName",xmlValue))
        scriptAlias <- unlist(xpathApply(subXMLDoc,path="/XMLobj/scriptAlias",xmlValue))
        processorConformanceClass <- unlist(xpathApply(subXMLDoc,path="/XMLobj/processorConformanceClass",xmlValue))

        if( length(scriptAlias) != 1 ) stop("ERROR : 'scriptAlias' is a mandatory tag")

        lst.script[[scriptAlias]] <- list()
        lst.script[[scriptAlias]]$scriptType <- scriptType
        lst.script[[scriptAlias]]$scriptFileName <- scriptFileName
        lst.script[[scriptAlias]]$processorConformanceClass <- processorConformanceClass

        free(subXMLDoc)
      }
    }


    # --------------------------------------------------
    # prima scorri i run
    # --------------------------------------------------
    subXML <- xpathApply(objXML,path="/xml/XMLobj[@objType='run']")
    lst.run <- list()
    if( length(subXML) > 0 ) {
      for( ct in 1:length(subXML)) {
        subXMLDoc <- xmlDoc(subXML[[ct]])
        runAlias <- unlist(xpathApply(subXMLDoc,path="/XMLobj/runAlias",xmlValue))
        scriptAlias <- unlist(xpathApply(subXMLDoc,path="/XMLobj/scriptAlias",xmlValue))
        dataSourceAlias <- unlist(xpathApply(subXMLDoc,path="/XMLobj/dataSourceAlias",xmlValue))
        outXMLDescriptorType <- unlist(xpathApply(subXMLDoc,path="/XMLobj/outXMLDescriptorType",xmlValue))
        outXMLDescriptorFileName <- unlist(xpathApply(subXMLDoc,path="/XMLobj/outXMLDescriptorFileName",xmlValue))

        if( length(scriptAlias) != 1 ) stop("ERROR : 'scriptAlias' is a mandatory tag")

        lst.run[[runAlias]] <- list()
        lst.run[[runAlias]]$scriptAlias <- scriptAlias
        lst.run[[runAlias]]$dataSourceAlias <- dataSourceAlias
        lst.run[[runAlias]]$outXMLDescriptorType <- outXMLDescriptorType
        lst.run[[runAlias]]$outXMLDescriptorFileName <- outXMLDescriptorFileName

        free(subXMLDoc)
      }
    }

    if( length(lst.script) != 1 ) stop("ERROR : in the current version exactly one objType is needed")

    return(
      list( "lst.dataSource" = lst.dataSource,
            "lst.other" = lst.other,
            "lst.script" = lst.script,
            "lst.run" = lst.run
            )
    )

  }

  set.current.state <- function( toState ) {
    global.current.state <<- toState
  }
  get.current.state <- function( toState ) {
    return( global.current.state )
  }

  # -----------------------------------------------------------
  # Costruttore
  # -----------------------------------------------------------
  constructor <- function( input_folder.dir , output_folder.dir , tmp_folder.dir,
                         log_folder.dir, cache_folder, sync_folder.dir ) {

    if( is.na(tmp_folder.dir) ) stop("Error: tmp_folder.dir must be specifified ")
    if( is.na(input_folder.dir) ) stop("Error: input_folder.dir must be specifified ")
    if( is.na(output_folder.dir) ) stop("Error: output_folder.dir must be specifified ")


    global.lst.input_folder <<- list("path" = input_folder.dir)
    global.lst.output_folder <<- list("path" = output_folder.dir)
    global.lst.log_folder <<- list( "path" = log_folder.dir )
    global.lst.tmp_folder <<- list( "path" = paste(c(tmp_folder.dir,"/run"),collapse = ''),
                                    "pathEnv" = paste(c(tmp_folder.dir,"/envs"),collapse = '')
                                    )
    global.sync.tmp_folder <<- list( "path" = sync_folder.dir )
    global.lst.cache_folder <<- list( "path" = cache_folder )

    cleanUp.tmp.folders()

    # if(dir.exists(global.lst.tmp_folder$path)== FALSE) { dir.create(global.lst.tmp_folder$path) }
    # if(dir.exists(global.lst.tmp_folder$pathEnv)== FALSE) { dir.create(global.lst.tmp_folder$pathEnv) }
    # if(dir.exists(global.lst.tmp_folder$path)== FALSE) stop("ERROR: global.lst.tmp_folder$path not created")
    # if(dir.exists(global.lst.tmp_folder$pathEnv)== FALSE) stop("ERROR: global.lst.tmp_folder$pathEnv not created")


    # set the machine to state READY
    set.current.state("READY")

  }
  cleanUp.tmp.folders <- function(run = TRUE, temp = TRUE) {
    if(run==TRUE ) {
      if(dir.exists(global.lst.tmp_folder$pathEnv)) unlink( global.lst.tmp_folder$pathEnv,recursive = T)
      if(dir.exists(global.lst.tmp_folder$pathEnv)== FALSE) { dir.create(global.lst.tmp_folder$pathEnv) }
      if(dir.exists(global.lst.tmp_folder$pathEnv)== FALSE) stop("ERROR: global.lst.tmp_folder$pathEnv not created")
    }
    if(temp==TRUE) {
      if(dir.exists(global.lst.tmp_folder$path)) unlink( global.lst.tmp_folder$path,recursive = T)
      if(dir.exists(global.lst.tmp_folder$path)== FALSE) { dir.create(global.lst.tmp_folder$path) }
      if(dir.exists(global.lst.tmp_folder$path)== FALSE) stop("ERROR: global.lst.tmp_folder$path not created")
    }
  }
  # ===========================================================
  constructor(input_folder.dir = input_folder.dir, output_folder.dir = output_folder.dir, cache_folder = cache_folder.dir,
              log_folder.dir=log_folder.dir, tmp_folder.dir=tmp_folder.dir, sync_folder.dir=sync_folder.dir)
  # ===========================================================

  return( list(
    "processor"=processor,
    "set.current.state"=set.current.state,
    "get.current.state"=get.current.state,
    "start"=start
  ))

}
