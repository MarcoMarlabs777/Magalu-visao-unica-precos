STORAGE_CLOUD = 'gcs'

if STORAGE_CLOUD == 'gcs':
    from .gcs import (
        check_if_file_exists,
        is_path_delta_lake,
        remove_file,
        get_list_of_files_in_manifest,
        count_files_on_manifest,
        mark_dataset_to_repartition,
    )
elif STORAGE_CLOUD == 'magalu':
    from .magalu import (
        check_if_file_exists,
        is_path_delta_lake,
        remove_file,
        get_list_of_files_in_manifest,
        count_files_on_manifest,
        mark_dataset_to_repartition,
    )
