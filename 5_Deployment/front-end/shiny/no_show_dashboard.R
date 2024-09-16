
library(shiny)
library(shinythemes)
library(shinyjs)
library(odbc)
library(DT)
library(glue)

source('./config.R')

# Function to get weekday name in Dutch
weekdays_in_dutch <- function(date) {
  # Get the English weekday name
  english_weekday <- weekdays(date)
  
  # Mapping of English to Dutch weekday names
  weekday_mapping <- c(
    "Sunday" = "zondag",
    "Monday" = "maandag",
    "Tuesday" = "dinsdag",
    "Wednesday" = "woensdag",
    "Thursday" = "donderdag",
    "Friday" = "vrijdag",
    "Saturday" = "zaterdag"
  )
  
  # Return the Dutch weekday name
  return(weekday_mapping[english_weekday])
}

fetch_day_number <- function(date) {
    day <- format(date, "%d")
    return(sub("^0+", "", day))  # remove leading whitespaces
}

month_in_dutch <- function(date) {
    month <- format(date, "%m")

    month_mapping <- c(
        "01" = "januari",
        "02" = "februari",
        "03" = "maart",
        "04" = "april",
        "05" = "mei",
        "06" = "juni", 
        "07" = "juli",
        "08" = "augustus",
        "09" = "september",
        "10" = "oktober",
        "11" = "november",
        "12" = "december" 
    )
    
    # Return the Dutch month name
    return(month_mapping[month])
}

preprocess_column <- function(df) {
    #' changes the column order, column names and values of the input fields

    df = df[,c( 'STARTDATEPLAN', 'STARTTIMEPLAN', 'SPECIALISM', 'LOCATIE', 'PATIENTNR', 'NAAM', 'GEBDAT', 'GESLACHT', 'TELEFOON', 'GEBELD', 'STATUS')]
    colnames(df) <- c('Datum', 'Tijd', 'Specialisme', 'Afspraak\nlocatie', 'Patientnr', 'Naam', 'Geboorte\ndatum', 'Geslacht', 'Telefoonnummer', 'Gebeld', 'Status')

    df$Gebeld <- ifelse((is.na(df$Gebeld) | df$Gebeld == 'Nee'), 'Nee', 'Ja')
    df$Status[is.na(df$Status)] <- "\u00A0"

    return(df)
}

obtain_data <- function() {
    #' fetches the data from the database
    con <- con <- dbConnect(odbc(), Driver = db_driver, Server =db_server,  Database = db_database, UID = db_uid,PWD = db_password, Port = db_port)

    data <- dbGetQuery(con,     paste0("SELECT STARTDATEPLAN, STARTTIMEPLAN, SPECIALISM, PATIENTNR, NAAM, GEBDAT, GESLACHT, TELEFOON, GEBELD, STATUS, LOCATIE
                                FROM NoShowPreds_curr
                                WHERE GROUP_AB = 'Intervention'
                                AND SPECIALISM =  '", spec, "'" ))
    
    data <- preprocess_column(data)

    dbDisconnect(con)

    return(data)
}

write_update_to_db <- function(data) {
    #' writes the feedback (Gebeld, status) to the database
    con <- con <- dbConnect(odbc(), Driver = db_driver, Server =db_server,  Database = db_database, UID = db_uid,PWD = db_password, Port = db_port)
    
    update_query <- glue("
    UPDATE 
        NoShowPreds_curr
    SET 
        GEBELD = '{data$Gebeld}', 
        STATUS = '{data$Status}'
    WHERE 1= 1 
        AND PATIENTNR = '{data$Patientnr}'
        AND STARTDATEPLAN = '{data$Datum}'
        AND STARTTIMEPLAN = '{data$Tijd}'
        AND SPECIALISM = '{data$Specialism}' 
        ")
   
    rs <- dbSendQuery(con, update_query)

    # tidy up
    dbClearResult(rs)
    dbDisconnect(con)

    return(data)

}

db_data <- obtain_data()

# -- SERVER -- #
server <- function(input, output){

    # create data objects
    db_data <- obtain_data()
    data <- reactiveVal(db_data)
    selectedRow <- reactiveVal(1)
    # render the overview table
    
    output$overviewTable <- renderDT({
        if (nrow(data()) > 0) {
            return(datatable(data()[, colnames(data())[!(colnames(data())  %in% c('Telefoonnummer', 'Specialisme'))]], 
                options=list(ordering=FALSE, lengthChange=FALSE, order=list(list(6, 'desc')), pageLength=50, dom='t'),#, lengthMenu=list(c(10, 25, 50, 100))),
                selection=list(mode='single', selected=selectedRow(), target='row', toggleable= FALSE)))
        } else {
            selectedRow(NULL)
            table <- datatable(data=as.data.frame(list("NULL" = c("Geen afspraken")), row.names=NULL, col.names=c("none"), options=list(selection='none')))
            return(table)
        }
    })

    # # changes the datatable based on the selected specialism
    # observeEvent(input$specialism_select,{

    #     # if input is not Alles, apply filtering
    #     if (input$specialism_select != "Alles"){
    #         newData <- db_data[db_data$Specialisme == input$specialism_select,]
    #     } else {
    #         newData <- db_data
    #     }
    #     data(newData)
    
    # }
    # )

    # updates the selectedRow reactive value
    observeEvent(input$overviewTable_rows_selected, {
        if (input$overviewTable_rows_selected != selectedRow())
            selectedRow(input$overviewTable_rows_selected)
    })

    # when the called dropdown menu gets changed, change this in the datatable and write it to the db
    observeEvent(input$called, {
        if (!is.null(selectedRow())) {
            # get row index of selected row
            rowIndex <- rownames(data())[selectedRow()]

            # update the data
            newData <- data()
            newData[rowIndex, 'Gebeld'] <- input$called
            db_data[rowIndex, 'Gebeld'] <<- input$called
            data(newData)

            # write input to database
            write_update_to_db(newData[rowIndex, ])
        }
    })

    output$tel_number <- renderUI({
        if (!is.null(selectedRow())) {
            rowIndex <- rownames(data())[selectedRow()]
            HTML(gsub(";", "<br/>", data()[rowIndex, 'Telefoonnummer']))
        }
    })

    output$name <- renderUI({
        if (!is.null(selectedRow())) {
            rowIndex <- rownames(data())[selectedRow()]
            data()[rowIndex, 'Naam']
        }
    })

    # change the called dropdown menu based on the selected patient
    output$called_select <- renderUI({
        if (!is.null(selectedRow())) {
            rowIndex <- rownames(data())[selectedRow()]
            selectedData <- data()[rowIndex, ]
            selectInput("called", "Gebeld:", c("Nee", "Ja"), selected = selectedData$Gebeld)
        }
    })

    # when the status dropdown menu gets changed, change this in the datatable and write it to the db
    observeEvent(input$status, {
        if (!is.null(selectedRow())) {
            # get index of selected row
            rowIndex <- rownames(data())[selectedRow()]

            # update data based on input
            newData <- data()
            newData[rowIndex, 'Status'] <- input$status
            db_data[rowIndex, 'Status'] <<- input$status
            data(newData)

            # write update to database
            write_update_to_db(newData[rowIndex, ])
        }
    })

    observeEvent(input$refresh, {
        db_data <<- obtain_data()
        data <<- reactiveVal(db_data)
        shinyjs::refresh()

    })

    # change the called dropdown menu based on the selected patient
    output$status_select <- renderUI({
        if (!is.null(selectedRow())) {
            rowIndex <- rownames(data())[selectedRow()]
            selectedData <- data()[rowIndex, ]
            selectInput("status", "Status:", c("\u00A0", "Afspraak behouden", "Afspraak geannuleerd of verzet", "1ste keer niet bereikbaar", "2de keer niet bereikbaar", "2de keer niet bereikbaar (VM ingesproken)"), selected=selectedData$Status)
        }
    })
}

css <-  "
        :root {
            --dt-row-selected:250, 165, 112;
        }
        .dataTables_length label,
        .dataTables_filter label,
        .dataTables_info {
            color: white!important;
            font-size: 12px;
        }
        .dataTables_wrapper tbody tr.selected {
            pointer-events: none;
            background-color: white !important; 
            color: white; 
        }
        .dataTables_wrapper table {
            color: white; 
            // font-size: 12px;
        }
        .table.dataTable tbody td.active, .table.dataTable tbody tr.active td {
            background-color: red!important;}
        .tabbable > .nav > li > a         {color: white}
        .tabbable > .nav > li.active> a   {color: white}
        
        .paginate_button {
            background: white !important;
        }
        .table.dataTable tbody tr.selected {
            pointer-events: none
        }
        h3 {
            font-weight: bold;
        }
        hr {
            border-top: 2px solid #ed6814;
        }
"

## UI ##
ui <- fluidPage(
    shinyjs::useShinyjs(),
    theme = shinytheme("darkly"),
    tags$style(HTML(css)),
    # title
    titlePanel(
        div(
            h1(paste0("No Show Dashboard ", spec), style = "margin: 15;"),
            )
    ),
    
    # white space
    fluidRow(br(), br()),
    
    # main contents
    fluidRow(
            column(8,
                fluidRow(
                    style="margin: 10px; padding: 2px; margin-bottom:0px !important; color:  #ed6814",
                    h3(paste0('Bellijst voor afspraken op ', weekdays_in_dutch(db_data['Datum'][[1]][1]), ' ', fetch_day_number(db_data['Datum'][[1]][1]), ' ', month_in_dutch(db_data['Datum'][[1]][1])))
                ),
                fluidRow(
                    style="border-style: solid; border-color: #ed6814; border-radius:  10px; margin: 10px; padding: 20px; margin-top: 0px", 
                    # selectInput("specialism_select", "Specialisme", append("Alles", unique(db_data[,"Specialisme"])), selected="Alles"),
                    actionButton("refresh", "\u00A0 Refresh bellijst", icon=icon("refresh")),
                    br(), br(), br(),
                    DTOutput("overviewTable")
                )
        ),
        column(3,
            fluidRow(
                style="margin: 10px; padding: 2px; margin-bottom:0px !important;  color:  #ed6814",
                h3('Bellen')
            ),
            fluidRow(
                style="border-style: solid; border-color: #ed6814; border-radius:  10px; margin: 0px; padding: 20px; padding-top: 30px; padding-bottom: 30px; margin-top: 0px;", 
                h3(style="margin: 0px;, padding: 0px; font-weight: bold", uiOutput("name")),
                hr(),
                h3(style="font-weight: bold; font-size: 22px;", "Telefoonnummer(s):"),
                h4(style="", htmlOutput("tel_number")),
                hr(),
                h3(style="font-weight: bold; font-size: 22px;", "Acties:"),
                uiOutput("called_select"),
                uiOutput("status_select")
            )
        )
    )
)

# start application
shinyApp(ui, server)
