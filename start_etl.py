from kindle import unzip_data, pre_process_files, create_redshift_cluster, load_files, destroy_redshift_cluster

def main():
    
    unzip_data.main()
    pre_process_files.main()
    create_redshift_cluster.main()
    load_files.main()
    # destroy_redshift_cluster.main()

if __name__ == "__main__":
    main()
