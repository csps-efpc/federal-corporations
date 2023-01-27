library(xml2)
library(tidyverse)
library(stringr)
library(magrittr)
library(lubridate)
library(janitor)
library(stringr)
library(arrow)
library(glue)
library(httr)
library(purrr)

getwd()

ISED_API_LAST_CaLLED <- Sys.time()
ISED_API_MIN_WAIT_TIME_SEC <- 0.5
ISED_API_URL_GLUE <- 'https://ised-isde.canada.ca/cc/lgcy/api/corporations/{corporation_id}.json?lang={.lang}'



#' Get a URL from ISED all requests to ISED come here so we can make sure not to get banned!
#'
#' @param url a location that 
#'
#' @return
#' @export
#'
#' @examples
ised_api_all_requests <- function(url){
  
  
  if (difftime(Sys.time(), ISED_API_LAST_CaLLED, units = 's') |> as.double() < ISED_API_MIN_WAIT_TIME_SEC){
    Sys.sleep(ISED_API_MIN_WAIT_TIME_SEC)
  }
  
  
  r <- httr::GET(url)
  c <- content(r)
  c
}





#' Opens you web browser with the link to the open data, that also links to the API documentation
#'
#' @param url a location to open your browser
#'
#' @return
#' @export
#'
#' @examples 
#' ised_api_open_web_page()
ised_api_open_web_page <- function(url = 'https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2'){
  browseURL(url)
}




#' Downloads The ZIP File from ISED, extracts all the file, to a temp directory,  return the list of files extrcted
#'
#' @param url location of ZIP file for download
#' @param tmp_dir location where zip file should be downloaded
#' @param temp_fn name of the zip file after download
#'
#' @return
#' @export
#'
#' @examples
ised_api_download_bulk_zip <- function(url = 'https://ised-isde.canada.ca/cc/lgcy/download/OPEN_DATA_SPLIT.zip', 
                                       tmp_dir = tempdir(TRUE),
                                       temp_fn = tempfile(tmpdir = tmp_dir, fileext = '.zip')
                                       ){
  #temp_fn <- file.path(tmp_dir, 'file6001bd15635.zip')
  download.file(url,temp_fn)
  dir(temp_fn)
  
  unzip(temp_fn, exdir = tmp_dir, overwrite = TRUE )
  fns <- unzip(temp_fn, exdir = tmp_dir, list  = TRUE ) |> pull(Name)
  file.path(tmp_dir, fns)
}





#' get corporation information from corp_id
#'
#' @param corporation_id a corporation ID
#' @param .lang string either 'eng' or 'fra'
#' @param url_glue format of the URL will be passed to glue::glue
#'
#' @return
#' @export
#'
#' @examples
#'    ised_api_corporation_id(2026261, .lang = 'eng')
#' 
ised_api_corporation_id <- function(
                                corporation_id, 
                                .lang, 
                                url_glue = ISED_API_URL_GLUE
                            ){

  
  glue::glue(url_glue) |>
    ised_api_all_requests()
}







#' Extracts a value from a content returned from url
#'
#' @param c  c Is the content from a response to a GET request to ISED API
#' @param name_singular  name_singular  name of tag to extract
#'
#' @return
#' @export
#'
#' @examples
extract_by_singular_nm <- function(c, name_singular){
  simple_nm <- 
    c |> map(~{
      if (name_singular %in% names(.x)){
        .x[[name_singular]] |> unlist()
      }
    }) |> 
    unlist() |> 
    paste0(collapse = ' ')
  tibble(nm = simple_nm)
}







#' get the name of the acts
#'
#' @param c Is the content from a response to a GET request to ISED API
#' @param name_singular  name of tag to extract
#'
#' @return
#' @export
#'
#' @examples
extract_act_code <- function(c, name_singular = 'act'){
  extract_by_singular_nm(c = c, name_singular = name_singular)
}



#' get the name of the status
#'
#' @param c  Is the content from a response to a GET request to ISED API
#' @param name_singular name of tag to extract
#'
#' @return
#' @export
#'
#' @examples
extract_status_code <- function(c, name_singular = 'status'){
  extract_by_singular_nm(c = c, name_singular = name_singular)
}






#' get the name and the type of name from the status
#'
#' @param c Is the content from a response to a GET request to ISED API
#'
#' @return
#' @export
#'
#' @examples
extract_name_code <- function(c){
  c |> 
    map_dfr(~{
    .x[['corporationNames']] |> 
      map_dfr(~{
        tibble(
          name = .x[['CorporationName']][['name']],
          nm = .x[['CorporationName']][['nameType']]
        )
      })
  })
}





#' Try to get some codes as words instead of numbers
#'
#' @param dat  list of Data Frame of existing tables with a numeric 'code' column
#' @param n_examples_per_code how many examples to get from each example of a code
#' @param all_langs 3 letter languages strings to check in the GET request
#' @param plurals vector of plurals or table names to extract
#' @param ext_funs vector of functions to do the extractions
#'
#' @return
#' @export
#'
#' @examples
ised_api_get_codes_examples <- function(
                                  dat,
                                  n_examples_per_code = 2,
                                  all_langs = c('eng', 'fra'),
                                  plurals = c('acts' ,         'statuses'         ,  'names'),
                                  ext_funs = c(extract_act_code,  extract_status_code,  extract_name_code)
                              ){
    purrr::map2_dfr(plurals,  ext_funs,  
      \(name_tbl, extraction_function){
        #name_tbl = 'names'
        #extraction_function = extract_name_code
        dat[[name_tbl]] |> 
          group_by(code) |> 
          sample_n(n_examples_per_code, replace = TRUE) |> 
          distinct() |>
          purrr::pmap_dfr(\(corporation_id, code, ...){
            #print(corporation_id)
            all_langs |> purrr::map_dfr(\(.lang){
              #.lang = 'eng'
              
              
              c <- ised_api_corporation_id(corporation_id = corporation_id, .lang = .lang)
              
              extraction_function(c) |>
                mutate(lang  = .lang)
            }) |> 
              mutate(corporation_id = corporation_id, 
                     code = code)
          }) |>
          mutate(tbl = name_tbl)
      }) 
}





#' Takes many dataframes, and row_bind them into one, then save the result, and return a list of dataframe that were saved.
#'
#' @param out_dir output directory
#' @param out_glue passed to glue::glue to help determin the ouput file name
#' @param pattern_names patterns of names to look for and output
#' 
#' @return
#' @export
#'
#' @examples
#'      bring_togeather_dataframes(
#'                        fns = file.path('data', 'OPEN_DATA_SPLIT') |> list.files(pattern = 'OPEN_DATA_.*_names.feather', full.names = TRUE),
#'                        pattern_names = 'names'
#'                      )
#' 
#' 
#' 
bring_togeather_dataframes <- function(
                                  fns,
                                  out_dir = file.path('data'),
                                  out_glue = '{.x}.feather',
                                  pattern_names = c('acts', 'statuses', 'activities', 'names', 'addresses') 
                                ){
  #fns = list.files(file.path('data','OPEN_DATA_SPLIT'), pattern = 'OPEN_DATA_.*_.*.feather', full.names = TRUE)
  pattern_names |>
    map(~{
      #.x = 'names'
      
      .dat <- 
        fns |> 
        str_subset(.x)  |> 
        map_dfr(read_feather)

      
      new_fn <- file.path(out_dir, glue(out_glue))
      
      
      write_feather(.dat, new_fn)
      .dat
    }) |>
    set_names(pattern_names)
}



#' Download zip file from ISED unzip it then return the list of files that should be imported! 
#'
#' @param ... passed to ised_api_download_bulk_zip
#' @param pattern pattern of file names to pass back.
#'
#' @return
#' @export
#'
#' @examples
download_xml_files_for_import <- function(
                                      ...,  
                                      pattern = 'OPEN_DATA_.*xml' 
                                      ){
  ised_api_download_bulk_zip(...) |>
    str_subset(pattern = pattern)
}


#' From a list of XML files import them one at a time into a dataframe and resave it to that same directory.
#'
#' @param fns a vector of file names
#'
#' @return
#' @export
#'
#' @examples
import_xml_files <- function(fns = download_xml_files_for_import()
                             ){
  
  new_fns <- 
    fns |>
      purrr::map(\(.fn){
       #.fn <-  fns |> sample(1)
        print(.fn)
        x <- 
          .fn |>
          read_xml() |> 
          as_list()
        ##################################
        # Extract some tables
        tbls <- 
          c('acts', 'statuses', 'activities', 'names') |>
          purrr::map(\(.corp_sub_tag){
            x$corpcan$corporations |> 
              #sample(10) |>
              purrr::map_dfr(\(.corp){
                #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
                corporation_id =  as.integer(attr(.corp,'corporationId'))
                
                .corp[[.corp_sub_tag]] |>
                  purrr::map_dfr(\(.x){
                    attributes(.x) |> 
                      as_tibble() |>
                      mutate(corporation_id =  corporation_id, 
                             name = unlist(.x))
                  })
              })  
          }) |> set_names(c('acts', 'statuses', 'activities', 'names'))
        
        
        
        
         
        tbls[['addresses']] <- 
          x$corpcan$corporations |> #length()
          #sample(10) |> 
          purrr::map_dfr(\(.corp){
            #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
            corporation_id =  as.integer(attr(.corp,'corporationId'))
            .corp$addresses |>
              purrr::map_dfr(\(.addr){
                #.addr <- x$corpcan$corporation |> sample(1) |> extract2('corporation') |> extract2('addresses') |> sample(1) |> extract2('address')
                
                .addr_attr <- attributes(.addr)
                .addr_attr_dat <- 
                  .addr_attr[names(.addr_attr) != "names"] |> 
                  as_tibble() |>
                  mutate(corporation_id =  corporation_id)
                
                .addr_dat <- 
                  .addr |> map_dfc(\(.x){
                    .x_code <- attr(.x,"code")
                    if (is.null(.x_code)){
                      unlist(.x)
                    }else{
                      .x_code
                    }
                  })
                
                bind_cols(.addr_dat, .addr_attr_dat)
              }) |>
              mutate(corporation_id =  as.integer(attr(.corp,'corporationId')))
          }) |>
          {\(.){
            select(., order(colnames(.)))
          }}() |>
          #select(order(colnames(corporation_addressess))) |>
          unite(col = 'address_line', matches('^addressLine'), sep = ', ', remove = TRUE, na.rm = TRUE) |>
          janitor::clean_names() |>
          mutate(postal_code = stringr::str_remove_all(postal_code, ' ')) |> 
          mutate(current  = stringr::str_to_upper(current) == 'TRUE')
        
        
        tbls |> 
          names() |>
          purrr::map_dfr(\(.tbl_nm){
            #.tbl_nm <- 'acts'
            print(.tbl_nm)
            .tbl <- tbls[[.tbl_nm]] |> janitor::clean_names()
            .tbl[['expiry_date']] <- if ('expiry_date' %in% colnames(.tbl)  ){.tbl[['expiry_date']] |> lubridate::as_datetime()}
            .tbl[['effective_date']] <- if ('effective_date' %in% colnames(.tbl)  ){.tbl[['effective_date']] |> lubridate::as_datetime()}
            .tbl[['date']] <- if ( 'date' %in% colnames(.tbl)  ){.tbl[['date']] |> lubridate::as_datetime()}
            .tbl[['current']] <- if (  'current' %in% colnames(.tbl)  ){str_to_upper(.tbl[['current']]) == 'TRUE'}
           
            
            new_fn <- .fn |> tools::file_path_sans_ext() |> paste0('_', .tbl_nm, '.feather')
            .tbl |> write_feather(new_fn)
            list(fn = new_fn)
          })
      })
  new_fns
}








dat <- 
  import_xml_files() |> 
  bring_togeather_dataframes()


# 
# .corp_sub_tag = 'directorLimits'
# x$corpcan$corporations |> 
#   sample(10) |>
#   purrr::map_dfr(\(.corp){
#     #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
#     corporation_id =  as.integer(attr(.corp,'corporationId'))
#     .corp[[.corp_sub_tag]] |>
#       purrr::map_dfr(\(.x){
#         unlist(.x)
#       }) |> mutate() 
#     
#     z = .corp[[.corp_sub_tag]]
#     attr(z$directorLimit, "effectiveDate")
#     
#     
#    
#         #.x <- x$corpcan$corporation |> sample(1) |> extract2('corporation')|> extract2(.corp_sub_tag) |> sample(1)
#         attr(.x, 'current')
#         .x$directorLimit |> names()
#         attr_obj <- attributes(.x) 
#         print(names(attr_obj))
#         attr_obj[names(attr_obj) != 'names']  |> 
#           as_tibble() |>
#           mutate(corporation_id =  corporation_id)
#         
#         
#         
#           .x |> map_dfr(\(.y){
#             .y |> map_dfc(\(){
#               
#             })
#             length(names(.y))
#             })
#             print(.nm)
#             as_tibble(unlist(.y)) 
#           })
#             .y_code <- attr(.y,"code")
#             if (is.null(.y_code)){
#               unlist(.y)
#             }else{
#               .y_code
#             }
#           })        
#       })
#   })  
# 







# 
# 
# corporation_addressess
# 
# 
# 
# 
# 
# 
# 
# corporation_addressess |>
#   mutate(current  = stringr::str_to_upper(current) == 'TRUE') |> 
#   mutate(effective_date =
#            
#            
#            lubridate::as_datetime(effective_date)) |>
#   mutate(expiry_date = lubridate::as_datetime(expiry_date))
# 
# 
# 
# 
# corporation_buisness_numbers <- 
#   x$corpcan$corporations |> 
#   sample(100)  |> 
#   purrr::map_dfr(\(.corp){
#     #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
#     #print(names(.corp))
#     tibble(
#       corporation_id =  as.integer(attr(.corp,'corporationId')),
#       business_number = as.character(unlist(.corp$businessNumbers))
#     )
#   })
# corporation_buisness_numbers
# 
# 
# 
# 
# 
# 
# corporation_acts <- 
#   x$corpcan$corporations |> 
#   sample(100) |>
#   purrr::map_dfr(\(.corp){
#     #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
#     corporation_id =  as.integer(attr(.corp,'corporationId'))
#     
#     .corp[['acts']] |>
#       purrr::map_dfr(\(.x){
#         attributes(.x) |> 
#           as_tibble() |>
#           mutate(corporation_id =  corporation_id)
#       })
#   })
# 
# 
# 
# corporation_statuses <- 
#   x$corpcan$corporations |> 
#   sample(100) |>
#   purrr::map_dfr(\(.corp){
#     #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
#     corporation_id =  as.integer(attr(.corp,'corporationId'))
#     
#     .corp$statuses |>
#       purrr::map_dfr(\(.x){
#         attributes(.x) |> 
#           as_tibble() |>
#           mutate(corporation_id =  corporation_id)
#       })
#   })
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# x$corpcan$corporations |> 
#   sample(100)  |> 
#   purrr::map_dfr(\(.corp){
#     #.corp <- x$corpcan$corporation |> sample(1) |> extract2('corporation')
#     #print(names(.corp))
#     tibble(
#       corporation_id =  as.integer(attr(.corp,'corporationId')),
#       business_number = as.character(unlist(.corp$businessNumbers))
#     )
#   })
# 
# 
# 
# 
# 
# 
# corporation_addressess 
#     .corp$addresses |> 
#       purrr::map_dfr(\(.addr){
#         .addr
#       })
#     #.addr <- .corp_2$addresses |> sample(1)
#     .addr2 <- .addr$address
#     names(.addr2)
#     
#     .addr2 |> 
#       names() |>
#       map_dfc(\(.nm){
#         #.nm <- 'addressLine'
#         l = list()
#         l[[.nm]] <- unlist(.addr2[[.nm]])
#         l
#       })
#     
#     
#     tibble(
#       corporation_id =  as.integer(attr(.corp,'corporationId')),
#       names = as.character(unlist(.corp$names))
#     )
#   })
# 
# 
#     #.corp <- x$corpcan$corporations |> sample(1)    
#     .corp_2 <- .corp$corporation
#     print(as.character(attr(.corp_2,'corporationId')))
# 
#   })
#   
# .corp_2$addresses |> 
#   purrr::map_dfr(\(.addr){
#     .addr
#     })
#     #.addr <- .corp_2$addresses |> sample(1)
#     .addr2 <- .addr$address
#     names(.addr2)
#     
#     .addr2 |> 
#       names() |>
#       map_dfc(\(.nm){
#         #.nm <- 'addressLine'
#         l = list()
#         l[[.nm]] <- unlist(.addr2[[.nm]])
#         l
#       })
#     .addr2$careOf
#     .addr2 |> 
#       tibble() |> unnest_wider(.addr2)
#   })
# tibble(
#   address = 
# ) |> unnest_wider(address)
# tibble(
#   corporation_id =  as.integer(attr(.corp_2,'corporationId')),
#   address = 
# ) |> un
# 
