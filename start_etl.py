from kindle import *

unzip_data.main()
pre_process_files.main()
create-redshift-cluster.main()
load_files.main()
destroy-redshift-cluster.main()

