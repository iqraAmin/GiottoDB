
#' @name calculateOverlap
#' @title Calculate polygon overlapped features
#' @export
setMethod(
  'calculateOverlap', signature(x = 'dbSpatProxyData', y = 'dbSpatProxyData'),
  function(
    x, y,
    n_per_chunk = 1e5,
    remote_name = result_count(),
    poly_subset_ids = NULL,
    feat_subset_column = NULL,
    feat_subset_ids = NULL,
    count_info_column = NULL,
    ...
  )
  {

    # 1. perform poly and feat subsets if necessary #
    # --------------------------------------------- #

    if (!is.null(poly_subset_ids)) {
      # polys subset
      x@attributes[] <- x@attributes[] %>%
        dplyr::filter(poly_ID %in% poly_subset_ids)
      x[] <- x[] %>%
        dplyr::inner_join(dplyr::select(x@attributes[], 'geom'), by = 'geom')

    }
    if (!is.null(feat_subset_ids)) {
      if (is.null(feat_subset_column)) stop(
        GiottoUtils::wrap_txt('feat_subset_column must be provided if feat_subset_ids is given')
      )
      # points subset
      y[] <- y[] %>%
        dplyr::filter(dplyr::across(feat_subset_column) %in% feat_subset_ids)
    }


    # 2. setup chunking function #
    # -------------------------- #

    chunk_fun = function(x, y) {
      GiottoClass::calculateOverlap(
        x, y, verbose = FALSE, ...
      )
    }


    # 3. spatial chunking
    res <- chunkSpatApply(
      x = x,
      y = y,
      remote_name = remote_name,
      chunk_y = TRUE,
      n_per_chunk = n_per_chunk,
      fun = chunk_fun
    )


    # 4. output cleanup #
    # ----------------- #

    # 4.1 dbvect write in any missing features
    missing_pts <- which(!y$feat_ID_uniq %in% res$feat_ID_uniq)
    if (length(missing_pts > 0L)) {
      missing_sv <- as.points(y[missing_pts,])
      missing_sv <- missing_sv[, c('feat_ID', 'feat_ID_uniq')]
      dbvect(
        x = missing_sv,
        db = cPool(x),
        remote_name = remoteName(res),
        overwrite = 'append',
        return_object = FALSE
      )
    }

    # 4.2 cleanup doublecounts
    # strategy:
    #   flag dups as:           -1
    #   flag first in group as:  1
    #   flag overlapped pts as: 10 (highly preferred)
    # Find sum of flags, then pick the max value
    res[] <- res[] %>%
      dplyr::group_by(feat_ID_uniq) %>%
      dplyr::mutate(flag_dup = as.numeric(dplyr::n() > 1) * -1) %>%
      dplyr::mutate(flag_first = as.numeric(.uID == min(.uID, na.rm = TRUE))) %>%
      dplyr::mutate(flag_overlap = as.numeric(!is.na(poly_ID)) * 10L) %>%
      dplyr::mutate(flag_sum = flag_dup + flag_first + flag_overlap) %>%
      dplyr::filter(flag_sum == max(flag_sum, na.rm = TRUE)) %>%
      dplyr::select(-flag_dup, -flag_overlap, -flag_first, -flag_sum) %>%
      dplyr::ungroup()

    return(res)
  }
)

