import bs4 as bs
import requests
import urllib.request
import img2pdf
import sys, os, re
import logging, math
from datetime import datetime
import string, unicodedata
from multiprocessing import Pool
import multiprocessing

#we want to pull all the episodes and their short story
URL_TO_CRAWL = 'https://zp.com/'
LOGGING_LEVEL = logging.CRITICAL
IMG_FP = 'zp/img'
PDF_FP = 'zp/pdf'
CURR_PATH = os.getcwd() 

#init logger 
log_format = "%(asctime)s - %(lineno)-4s - %(levelname)-8s  - [%(name)s] - %(message)s"
logger = logging.basicConfig(level=LOGGING_LEVEL, format=log_format, datefmt='%d-%m-%y %H:%M:%S', filename = 'zp_log.log')
logger = logging.getLogger(__name__)


def send_requests(url):
	try:
		logger.info('trying to connect to the URL: {}'.format(url))
		return requests.get(url)
	except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
		logger.critical('Either the server is down, or you are not connected to the internet!')
		sys.exit(1)

def bs_body(response):
	logger.debug('in bs_body function')
	return bs.BeautifulSoup(response.text, 'lxml')

def pull_img_to_local(img_src, fp):
	logger.debug('in pull_img_to_local, downloading image: {}'.format(img_src))
	try:
		logger.debug('img Parameters {}, {}'.format(img_src, os.path.join(CURR_PATH, fp,img_src.split('/')[-1])))
		urllib.request.urlretrieve(img_src, os.path.join(CURR_PATH, fp,img_src.split('/')[-1]))
	#except (urllib.HTTPError, urllib.URLError):
	#	logger.error('Error while downloading the image. Proabably its moved???')
	except Exception as e:
		#logger.error('Unknown Error: {}'.format(e))
		logger.error(e)

def clean_filename(filename):
	valid_filename_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
	#filename = filename.replace(' ',' ')
	cleaned_filename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore').decode()
	return ''.join(c for c in cleaned_filename if c in valid_filename_chars)	

def convert_img_to_pdf(img_fp, pdf_fp, files, title):
	logger.info('in convert_img_to_pdf, converting file: {}'.format(files))
	try:
		logger.debug('filepath img: {}, filepath pdf: {}'.format(os.path.join(CURR_PATH, img_fp,filename),os.path.join(CURR_PATH, pdf_fp,filename )))
		#pdf_bytes = img2pdf.convert(os.path.join(CURR_PATH, img_fp,filename))
		pdf_bytes = img2pdf.convert(files)
		file = open(os.path.join(CURR_PATH, pdf_fp, clean_filename(title) + '.pdf'),"wb")
		file.write(pdf_bytes)
	except Exception as e:
		print(e)
		#logger.error('Unknown Error: {}'.format(e))
		logger.error(e)

def job_get_posts(post_links):	
	#awesome! we got all the links and the title for each post. 
	#the list though is not sorted, lets skip it for now
	#post_links =[('s','')]
	#print('job id:', job_id)

	post_cnt = 0
	for title, url in post_links:
		logger.info('Reading the post {}, url = {}'.format(title, url))
		print('{} Reading the post {}, url = {}'.format(multiprocessing.current_process(), title, url))
		resp = send_requests(url)
		div_tag = bs_body(resp).find('div', id='comic')
		#div = [div for div in divs if div.get('id') == 'comic']
		img_tags = [div for div in div_tag if div.name == 'img']
		img_names = []
		for img in img_tags:
			img_src = img['src']
			img_names.append(os.path.join(CURR_PATH, IMG_FP, img_src.split('/')[-1]))
			logger.debug('Post: {}, URL: {}, img_src: {}'. title, url, img_src)

			#download the image 
			pull_img_to_local(img_src, IMG_FP)

		#conver the image to pdf
		convert_img_to_pdf(IMG_FP, PDF_FP, img_names, title)
		
		post_cnt += 1
		# if post_cnt > 1: break

def dispatch_jobs(post_links):
	how_many_processes = 10
	print('Number of Threads: {}'.format(how_many_processes))
	num_of_post_per_thread = int(math.ceil(len(post_links)/how_many_processes))
	#post_links = range(0,422)
	post_links_splits = [post_links[i:i+num_of_post_per_thread-1] for i in range(0, len(post_links), num_of_post_per_thread)]
	p = Pool()
	#data = p.map(job_get_posts1, [post_links_split for post_links_split in post_links_splits ])
	#print(post_links_splits)
	print('Lenght of Split: {}'.format(len(post_links_splits)))
	data = p.map(job_get_posts, post_links_splits)
	p.close()
	#p.join()
	print(data)
	# jobs = []
	# for pls in post_links_splits:
	#  	j = multiprocessing.Process(target=job_get_posts1, args=(pls, ) )
	#  	j.start()
	#  	#pass
	# for j in jobs:
	# 	j.start()

def get_all_posts():
	response = send_requests(URL_TO_CRAWL)
	body = bs_body(response)
	option_tags = [ot for ot in body.find_all('option', class_ = 'level-0')]
	logger.debug(response.content)
	post_links = []
	logger.info('Pulling all the posts and corrosponding URLs')
	for ot in option_tags:
		logger.debug("{} - {}".format(ot.string, ot.attrs['value']))
		post_links.append((ot.string, ot.attrs['value']))

	logger.info('Pulled {} posts'.format(len(post_links)))
	print('Pulled {} posts'.format(len(post_links)))

	return post_links
	
			
if __name__ == '__main__':

	#track time
	start_time = datetime.now()
	print('Start Time: {}'.format(start_time.strftime('%d-%m-%y %H:%M:%S')))
	logger.info('Start Time: {}'.format(start_time.strftime('%d-%m-%y %H:%M:%S')))

	how_many_processes = 5
	print ('Current Recurssion Limit: 'sys.getrecursionlimit())
	if sys.getrecursionlimit() == 1000:
		sys.setrecursionlimit(10000)
		print('New Recursion Limit', sys.getrecursionlimit())
	#p = Pool(processes=how_many_processes)
	#try to connect to website
	logger.debug('main starts here')
	
	#we want to pull all the urls for all posts
	#post_links = ['s','x']
	post_links = get_all_posts()

	dispatch_jobs(post_links)

	print('End Time: {}'.format(datetime.now().strftime('%d-%m-%y %H:%M:%S')))
	print('Total Time Take: {}'.format(datetime.now()-start_time))
