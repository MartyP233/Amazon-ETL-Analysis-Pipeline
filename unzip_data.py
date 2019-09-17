# import requests
import zipfile
# import io

# download file
# zip_file_url = 'https://www.kaggle.com/bharadwaj6/kindle-reviews/downloads/kindle-reviews.zip/3'
# login_url = 'https://www.kaggle.com/account/login?phase=emailSignIn&returnUrl=%2Fbharadwaj6%2Fkindle-reviews%2Fversion%2F3'

# payload = {'username': 'martin.peak@ssc.govt.nz', 'password': 'yUUs$Wv$5:ykeMb'}

# # Use 'with' to ensure the session context is closed after use.
# with requests.Session() as s:
#     p = s.post(login_url, data=payload)
#     # print the html returned or something more intelligent to see if it's a successful login page.
#     print(p.text)

#     # An authorised request.
#     r = s.get(zip_file_url)
#     print(r.text)

# requests.post(zip_file_url, data=payload)
# r = requests.get(zip_file_url)
# z = zipfile.ZipFile(io.BytesIO(r.content))
# z.extract('kindle_reviews.csv', path='Data/')

# unzip local file

def unzip_file(zip_file, filename, directory):
    with zipfile.ZipFile(zip_file, 'r') as file:
        print(f'Extracting {filename}')
        result = file.extract(filename, path=directory)
        print(f'finished extracting {filename} at {result}')

def main():
    unzip_file('Data/downloads/kindle-reviews.zip', 'kindle_reviews.csv', 'Data/')
    unzip_file('Data/downloads/amazon-sales-rank-data-for-print-and-kindle-books.zip', 'amazon_com_extras.csv', 'Data/unzipped/')
    unzip_file('Data/downloads/amazon-sales-rank-data-for-print-and-kindle-books.zip', 'ranks_norm.zip', 'Data/unzipped/')

    zip_file = 'Data/unzipped/ranks_norm.zip'
    with zipfile.ZipFile(zip_file, 'r') as file:
        print(f"Extracting {zip_file}")
        file.extractall(path='Data/unzipped/')

if __name__ == "__main__":
    main()
