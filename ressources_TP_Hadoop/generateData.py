#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import os.path
import shutil
import random
import time




def getIntBetween(a,b):
    return random.randint(a,b)
    #return int(random.uniform(0,100000000))%b + a

def ecrireligneBooble(f):
    

    jj=getIntBetween(1, 28)
    mm=getIntBetween(1,12)
    aaaa=getIntBetween(2000,2015)
    date=str(jj)+"_"+str(mm)+"_"+str(aaaa)
     
 
    hh=getIntBetween(0,23)
    mm=getIntBetween(0,59)
    ss=getIntBetween(0,59)
    heure=str(hh)+"_"+str(mm)+"_"+str(ss)
	
    ip_adress=str(getIntBetween(1,255))+"."+str(getIntBetween(1,255))+"."+str(getIntBetween(1,255))+"."+str(getIntBetween(1,255))
	
    nb_keywords=getIntBetween(1,5)
    keywords=""
    i=0
    while i < nb_keywords:
        id_word="word"+str(getIntBetween(0,1024))
        if keywords== "":
            keywords=id_word
        else:
            keywords=keywords+"+"+id_word
        i+=1
	f.write(date+" "+heure+" "+ip_adress+" "+keywords+"\n")


cat_of_produit=dict()
def ecrireligneStereoPrix(f):
    jj=getIntBetween(1, 28)
    mm=getIntBetween(1,12)
    aaaa=getIntBetween(2000,2015)
    date=str(jj)+"_"+str(mm)+"_"+str(aaaa)
     
 
    hh=getIntBetween(0,23)
    mm=getIntBetween(0,59)
    ss=getIntBetween(0,59)
    heure=str(hh)+"_"+str(mm)+"_"+str(ss)
    
    magasin="magasin"+str(getIntBetween(0,50))
	
    prix=str(random.uniform(1,1000))
    
    produit="produit"+str(getIntBetween(0,1000))
    
    if cat_of_produit.has_key(produit):
        categorie=cat_of_produit[produit]
    else:
        categorie="categorie"+str(getIntBetween(0,30))
        cat_of_produit[produit]=categorie
    
    f.write(date+" "+heure+" "+magasin+" "+prix+" "+produit+" "+categorie+"\n")
    
def  ecrireligneLastFM(f):
    user_id=str(getIntBetween(0,1000000))
    trackid=str(getIntBetween(0,10000))
    localListening=str(getIntBetween(0,200))
    radioListening=str(getIntBetween(0,300))
    skip=str(getIntBetween(0,50))
    f.write(user_id+" "+trackid+" "+localListening+" "+radioListening+" "+skip+"\n")
    
    
def ecrireligne(f,type_donnee):   
    if type_donnee=="Booble":
        ecrireligneBooble(f)
    if type_donnee =="StereoPrix":
        ecrireligneStereoPrix(f)
    if type_donnee == "LastFM":
        ecrireligneLastFM(f)
    

def ecrire(path_fichier,size_fichier,type_donnee):
    with open(path_fichier, 'w') as f:
        #size_actuelle=os.system('du -BK '+path_fichier+'| cut -f1 -dK')
        size_actuelle=os.path.getsize(path_fichier)
        size_finale=size_fichier*1024*1024
        while(size_actuelle < size_finale):
            ecrireligne(f,type_donnee)
            size_actuelle=os.path.getsize(path_fichier)

if __name__ == '__main__':
    types_donnees=["Booble","StereoPrix","LastFM"]
    usage = "usage: %s <%s> <size_fichier(Mo)> <nb_fichiers>" %(sys.argv[0], str(types_donnees))
   
    if(len(sys.argv) != 4):
        print "%s" %(usage)
        exit(1)
    random.seed(5)
    type_donnee=sys.argv[1]
    size_fichier=int(sys.argv[2])
    nb_fichiers=int(sys.argv[3])

    if type_donnee not in types_donnees:
        print "%s" %(usage)
        exit(1)
   
    name_dir=type_donnee+"_"+str(size_fichier)+"_"+str(nb_fichiers)
    
    if os.path.exists(name_dir):
        shutil.rmtree(name_dir)
    os.makedirs(name_dir)
    
    i=0
    while (i < nb_fichiers):
        nom_fichier=type_donnee+"_"+str(i)
        ecrire(name_dir+"/"+nom_fichier,size_fichier,type_donnee)
        i+=1
    
    
    
    
