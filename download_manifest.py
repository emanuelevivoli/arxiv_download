import re
import sys
import json
import subprocess
import traceback
import xml.etree.ElementTree as ET

import csv
from collections import defaultdict

import json
import ast

from functools import reduce

def get_number(meta_tax_unordered, category_id):
  print(category_id)
  category_name = category_id.split('/')[0]
  print(meta_tax_unordered)
  index = meta_tax_unordered.index(category_name)
  return f'{index:04}'

def input_snapshot_to_json(path_to_file, meta_tax_unordered):
  df_data_id = {}
  df_data_nid = {}
  ls_data = []
  last = 0
  with open(path_to_file) as json_file:
    for j,line in enumerate(json_file):
      single_dict = {}
      # 1382795
      # 1796911
      if j > 1382793:
        break

      if int(j/1382793*100) != last:
        last = int(j/1382793*100)
        print(' ['+ str(last) +'%] '+str(j)+'/1382793')
      row = json.loads(line)
      single_dict['id'] = row['id']
      single_dict['nid'] = '20'+row['id'] if '.' in row['id'] else ('20'+str(row['id'][-7:-3]) if str(row['id'][-7:-3]) < "50" else '19'+str(row['id'][-7:-3]))+'.'+get_number(meta_tax_unordered, row['id'])+str(row['id'][-3:])
      single_dict['categories'] = row['categories']
      df_data_id[single_dict['id']] = single_dict
      df_data_nid[single_dict['nid']] = single_dict
      ls_data.append(single_dict['nid'])
  return df_data_id, df_data_nid, sorted(ls_data)

def save_dict_to_csv(file_dict, filt_to_save):
  w = csv.writer(open(filt_to_save, "w"))
  for key, val in file_dict.items():
        w.writerow([key, val])

def save_list_to_txt(file_list, filt_to_save, to_set=True, sort=True):
  unique_list = file_list
  if to_set: 
    unique_list = set(unique_list)
  if sort:
    unique_list = sorted(unique_list)
  with open(filt_to_save, 'w') as f:
    for item in unique_list:
        f.write("%s\n" % item)

def read_dict_list_from_csv(path_to_file):
  meta_df = {}
  with open(path_to_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    # header = next(csvreader)
    for row in csvreader:
      first, second = row[0], row[1]
      if first in meta_df:
          meta_df[first].append(second)
      else:
          meta_df[first] = [second] 
      
  return meta_df

def read_dict_from_csv(path_to_file):
  meta_df = {}
  with open(path_to_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    # header = next(csvreader)
    for row in csvreader:
      first, second = row[0], row[1]
      
      meta_df[first] = second
      
  return meta_df

def read_list_from_txt(path_to_file):
  meta_ls = []
  with open(path_to_file, 'r') as txtfile:
    for row in txtfile:
      meta_ls.append(row.strip())
  return meta_ls
  
def input_categories_to_list(path_to_file):
  in_ls = []
  with open(path_to_file, 'r') as txtfile:
    for row in txtfile:
      line = txtfile.readline()
      if line[0] != '#' and line != '\n':
        in_ls.append(line.strip())

  return sorted(in_ls)

def categories_to_ordered_list(path_to_file):
  meta_ls = []
  unordered = []
  with open(path_to_file, 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    header = next(csvreader)
    for row in csvreader:
      meta_ls.append([row[4].split('.')[0] if '.' in row[4] else row[4], row[1]])
      unordered.append(row[4])
  meta_ls = [list(item) for item in set(tuple(row) for row in meta_ls)]
  unordered = list(set(unordered))
  return sorted(meta_ls), unordered

def tar_categories_list(fname, fitem, litem, meta_snap_dict_id, meta_snap_dict_nid, meta_cat_list):
  fid = fitem if '.' in fitem else str(fitem[:-7])+'/'+str(fitem[-7:])
  lid = litem if '.' in litem else str(litem[:-7])+'/'+str(litem[-7:])
  
  # print('fname', fname)
  # print('findex', fid)
  # print('lindex', lid)

  fnid = ast.literal_eval(meta_snap_dict_id[fid])
  lnid = ast.literal_eval(meta_snap_dict_id[lid])

  findex = meta_cat_list.index(fnid['nid'])
  lindex = meta_cat_list.index(lnid['nid'])

  
  # print('findex', findex)
  # print('lindex', lindex)

  step_one = [meta_snap_dict_nid[meta_cat] for meta_cat in meta_cat_list[findex:lindex+1]]
  # print('1', step_one)
  
  step_two = [ast.literal_eval(meta_snap)['categories'] for meta_snap in step_one]
  # print('2', step_two)
  
  step_three = [ele.split(' ') for ele in step_two]
  # print('3', step_three)
  
  step_four = [[el.split('.')[0] if '.' in el else el for el in ele ] for ele in step_three]
  # print('4', step_four)
  
  step_five = reduce(lambda x,y: x+y, step_four)
  # print('5', step_five)
  
  step_six = set(step_five)
  # print('6', step_six)

  step_seven = list(step_six)
  # print('7', step_seven)

  # categories_set = set(reduce(lambda x,y: x+y,[[ele.split('.')[0] if '.' in ele  else ele for ele in ast.literal_eval(meta_snap)['categories'].split(' ')] for meta_snap in [meta_snap_dict_nid[meta_cat] for meta_cat in meta_cat_list[findex:lindex+1]]]))
  # categories_unique_list = list(categories_set)

  return step_seven if findex < lindex else None, fnid, lnid

def intersection(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 

def paper_to_download(cat_list, in_cat_list):
  inter = intersection(cat_list, in_cat_list)
  return False if not inter else True, inter


def main(**args):
  manifest_file = args['manifest_file']
  meta_snap_file = args['metadata_oai_snapshot']
  meta_cat_file = args['metadata_ext_categories']
  meta_tax_file = args['metadata_ext_taxonomy']
  
  in_categories = args['input_categories']

  year_choice = args['year_choice']
  month_choice = args['month_choice']
  seq_index_limit = args['seq_index_limit']
  seq_index_limit = f'{seq_index_limit:03}' if seq_index_limit else None
  
  mode = args['mode']
  out_dir = args['output_dir']
  log_file_path = 'logs/' + args['log_file']
  mode_file_uri = 'logs/' + args['uri_file']

  if mode != 'pdf' and mode != 'src':
    print('mode should be "pdf" or "src"')

  def get_file(fname, out_dir):
    cmd = ['s3cmd', 'get', '--requester-pays',
           's3://arxiv/%s' % fname, './%s' % out_dir]
    print(' '.join(cmd))
    #! uncomment to download the desired zips
    # subprocess.call(' '.join(cmd), shell=True)    
    

  log_file = open(log_file_path, 'a')
  uri_file = open(mode_file_uri, 'a')

  try:
    print("Check if files exist")
    meta_snap_dict_id = read_dict_from_csv('backups/metadata_snap_dict_id.csv')
    meta_snap_dict_nid = read_dict_from_csv('backups/metadata_snap_dict_nid.csv')
    meta_cat_list = read_list_from_txt('backups/metadata_cat_list_nid.txt')
    meta_tax_list = read_list_from_txt('backups/selected_categories.txt')
    print("Files exist, load them correctly")
    
  except IOError:
    print("Files don't exist")
    print("Re make those")
    meta_tax_list, meta_tax_unordered = categories_to_ordered_list(meta_tax_file)
    meta_snap_dict_id, meta_snap_dict_nid, meta_cat_list = input_snapshot_to_json(meta_snap_file, meta_tax_unordered)

    print("Save files for future")
    save_dict_to_csv(meta_snap_dict_id, 'backups/metadata_snap_dict_id.csv')
    save_dict_to_csv(meta_snap_dict_nid, 'backups/metadata_snap_dict_nid.csv')
    save_list_to_txt(meta_cat_list, 'backups/metadata_cat_list_nid.txt')
    save_list_to_txt(meta_tax_list, 'backups/selected_categories.txt', False, False)

  in_cat_list = input_categories_to_list(in_categories)
  meta_tax_list = read_dict_list_from_csv('tools/arxiv-metadata-ext-category.csv')

  total_size = 0
  total_money = 0
  # seq_index = 0

  try:
    for event, elem in ET.iterparse(manifest_file): #, events=("start", "end")):
      # print("[INFO] ", event)
      # print("[INFO] ", elem.tag)
      # print("[INFO] ", elem.text)
      
      tag = elem.tag
      value = elem.text

      if event == 'end':
            
        if tag != 'file':
          if value == None:
            raise Exception('None occured in start when tag != file at: ', fname, fitem, litem, tag)

          elif tag == 'filename' :
            fname = value # elem.text
          elif tag == 'first_item' :
            fitem = value # elem.text
          elif tag == 'last_item' :
            litem = value # elem.text
          elif tag == 'size' :
            size = int(value) # elem.text

        elif tag == 'file':

          cat_list, fnid, lnid = tar_categories_list(fname, fitem, litem, meta_snap_dict_id, meta_snap_dict_nid, meta_cat_list)
          
          # print(cat_list)
          # print(in_cat_list)

          if cat_list == None:
            raise Exception('None occured in categories list: first index > last index')

          
          choice, intersection = paper_to_download(cat_list, in_cat_list)
        
          #! uncomment to download the desired zips
          # get_file(fname, out_dir='%s/%s/' % (out_dir, mode))
          log_file.write(str(fname) + '\t' + str(choice) + '\t' + str(intersection) + '\t' + str(size / 1073741824)+' GB' + '\n')

          yyyymm = '19' + fname.split('_')[2] if fname.split('_')[2] > '50' else '20'+ fname.split('_')[2]
          year = yyyymm[:4]
          month = yyyymm[-2:]

          seq_num = fname.split('_')[3].split('.')[0]
          
          # print(yyyymm)
          # print(year_choice, year)
          # print(month_choice, month)
          # print('<' + seq_index_limit, seq_num)

          if(year_choice == year and month_choice == month and seq_num <= seq_index_limit if seq_index_limit else True):
            total_size += size
            total_money += (size / 1073741824) * 0.02

            uri_file.write(str(fname)+ '\n')

            print(str(total_size / 1073741824)+' GB - ' + str(total_money) +' €')
      
      elem.clear()
  except:
    traceback.print_exc()

  print('Finished')


if __name__ == '__main__':
  from argparse import ArgumentParser
  ap = ArgumentParser()
  ap.add_argument('--manifest_file', '-manifest', type=str, default='tools/arXiv_pdf_manifest_newindex.xml', help='The manifest file for downloading from arxiv. Obtain it from s3://arxiv/pdf/arXiv_pdf_manifest.xml using `s3cmd get --add-header="x-amz-request-payer: requester" s3://arxiv/pdf/arXiv_pdf_manifest.xml`')
  ap.add_argument('--metadata_oai_snapshot', '-metasnap', type=str, default='tools/arxiv-metadata-oai-snapshot.json', help='The input metadata snapshot file for checking the categories of papers in .tar file. Obtain it from Kaggle at `https://www.kaggle.com/Cornell-University/arxiv` and downloading `arxiv-metadata-oai-snapshot.json` (2.7 GB)')
  ap.add_argument('--metadata_ext_categories', '-metacat', type=str, default='tools/arxiv-metadata-ext-category.csv', help="The arxive papers' categories list. Each paper can have more than one category. Obtain it from Kaggle at `https://www.kaggle.com/steubk/arxiv-taxonomy-e-top-influential-papers/data?select=arxiv-metadata-ext-category.csv` ")
  ap.add_argument('--metadata_ext_taxonomy', '-metatacx', type=str, default='tools/arxiv-metadata-ext-taxonomy.csv', help="The arxive papers' categories list description. Obtain it from Kaggle at `https://www.kaggle.com/steubk/arxiv-taxonomy-e-top-influential-papers/data?select=arxiv-metadata-ext-taxonomy.csv` ")

  ap.add_argument('--input_categories', '-incat', type=str, default='categories.txt', help="The input categories list file. If it is empty or unexisted, the script won't download anything. This is to prevent unnecessary or wrong expenses.")
  
  ap.add_argument('--output_dir', '-o', type=str, default='data', help='the output directory')
  ap.add_argument('--mode', type=str, default='pdf', choices=set(('pdf', 'src')),
                  help='can be "pdf" or "src"')
  ap.add_argument('--log_file', default='log.txt', help='A file that logs the processed txt files')
  ap.add_argument('--uri_file', default='list_uri_to_download.txt', help='A file that contains uri to download')

  ap.add_argument('--year_choice', default='2020', help='The year you want to select')
  ap.add_argument('--month_choice', default='09', help='The month (in the year) you want to select')
  ap.add_argument('--seq_index_limit', type=int, default=None, help='The month (in the year) limit packages we want to download')

  args = ap.parse_args()
  main(**vars(args))