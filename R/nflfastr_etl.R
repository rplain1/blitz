update_nflfastR_db <- function(dbdir = '~/.db', force_rebuild = F) {
  nflfastR::update_db(dbdir = dbdir, force_rebuild = force_rebuild)
}
