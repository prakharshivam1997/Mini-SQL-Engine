import csv
import pandas
import sys
from prettytable import PrettyTable
data={}
prPrint=PrettyTable()
rList=['=','>','<','>=','<=']
def extractTable(Slist):      
    i=1
    tables=[]
    #print(Slist)
    while(i<len(Slist)):
        if(Slist[i]=="from"):
            break
        i=i+1
    i=i+1
    while i<len(Slist):
        if(Slist[i]=='where'):
            break
        else:
            tables.append(Slist[i])
        i=i+1
    tables=tables[0].split(',')
    #print(tables,"asd")
    return tables 




def valueAppender(q1,q2):
    for i in range(len(q2)):
        q1.append(int(q2[i]))
    #print(q1)

def valueEraser(temp,line):
    i=0
    for i in range(len(line)):
        temp.pop()

def tableSizeMap(tables,sizeMap):
    x=0
    col_size=0
    #print(len(tables))
    for x in range(len(tables)):
        with open(tables[x]+'.csv','r') as csv_file:
            csv_reader=csv.reader(csv_file)
            for line in csv_reader:
                col_size=col_size+1
        sizeMap[x]=col_size
        col_size=0 

def frameList(tables,reach,xtr,temp,sizeMap,cval):                              #frameList appends data into list
    with open(tables[reach]+'.csv','r') as csv_file:
        csv_reader=csv.reader(csv_file)
        for line in csv_reader:
            valueAppender(temp,line)
            if(reach==len(tables)-1):
                fl=[]
                valueAppender(fl,temp)
                data[cval]=fl
                cval=cval+1
                #print(fl)
                #print(cval)
                xtr.append(fl)
            else:
                frameList(tables,reach+1,xtr,temp,sizeMap,cval)
                sf=1
                v=reach+1
                while v <len(tables):
                    sf=sf*sizeMap[v]
                    v=v+1 
                cval=cval+sf
            valueEraser(temp,line)


def frameCreater(tables,col):                         #frameCreater creates the resultant dataframe
    temp=[]
    xtr=[]
    sizeMap={}
    #print(tables)
    tableSizeMap(tables,sizeMap)
    #print(sizeMap[2])
    #print(sizeMap)
    frameList(tables,0,xtr,temp,sizeMap,cval)



def columnIndexer(metaList,tables):                                # creates dictionary with column values and index
    x=0
    colmap={}
    i=0
    
    while i<len(metaList):
        z=i
        if(metaList[i]=='<begin_table>'):
            for t in tables:
                if(t==metaList[i+1]):
                    z=i+2
                    #print(t)
                    while(z<len(metaList) and metaList[z]!='<end_table>'):
                        colmap[metaList[z]]=x
                        x=x+1
                        #print(metaList[z])
                        z=z+1
        i=z+1
    return colmap
    #print(colmap)
    


def columnExtractor(Slist):
    column=[]
    result=[]
    for val in Slist:
        if(val=="Select" or val=="select"):
            continue
        elif(val=="from"):
            break
        else:
            column.append(val)
    #print(column)
    if(len(column)>1):
        for x in column:
            #print(x.split(','))
            temp=x.split(',')
            for t in temp:
                if(t!='' and t!=' '):
                    result.append(t)
        return result
    else:
        return column[0].split(',')
    #print(result)
    return result




def aggregateFunc(mydict,type,col):                  #calculates value of aggregate functions
    count_row=len(mydict)
    sum=0
    i=0
    #print(col)
    if(count_row==0):
        print("No value in data frame")
        exit()
    if(type=='sum'):
        while i<count_row:
            sum=sum+mydict[i][col]
            i=i+1
        return sum
    elif(type=='AVG' or type=='avg'):
        n=0
        sum=0
        i=0
        while i<count_row:
            sum=sum+mydict[i][col]
            n=n+1
            i=i+1
        return sum/n
    elif(type=='max' or type=="MAX"):
        maxi=mydict[0][col]
        i=1
        while( i< count_row):
            maxi=max(maxi,mydict[i][col])
            i=i+1
        return maxi
    elif (type=='min' or type=="MIN"):
        mni=mydict[0][col]
        while i< count_row:
            mni=min(mni,mydict[i][col])
        return mni 
    
    


def aggregateColStrip(str):                       #strips column value of aggregate function, returns column
    i=0
    #print(str)
    stx=''
    while(i<len(str)):
        if(str[i]==')'):
            break
        else:
            stx=stx+str[i]
        i=i+1
    return stx

def isAggregateFunc(column):
    aggr=['sum','AVG','max','count','SUM','avg','COUNT','min','MIN']
    for line in aggr:
        for c in column:
            if(c.find(line)==-1):
                continue
            else:
                return True
    return False



def columnPrinter(mydict,column,colmap,gflag):
    count_row = len(mydict)
    if(count_row==0):
        return
    #print(column)
    #print(colmap[column[0]])
    #print(colmap)
    #print("Here")
    col_list=[]
    if(isAggregateFunc(column)==True):
        #print("TE")
        for c in column:
            if(c[0:3]=='sum'):
                #print(c[0:3])
                strx=aggregateColStrip(c[4:])
                #print(strx)
                val=aggregateFunc(mydict,'sum',colmap[strx])
                col_list.append(val)
                print(val,end=" ")
            elif(c[0:3]=='avg' or c[0:3]=='AVG'):
                strx=aggregateColStrip(c[4:])
                val=aggregateFunc(mydict,'avg',colmap[strx])
                col_list.append(val)
                print(val,end=" ")
            elif(c[0:3]=='max' or c[0:3]=='MAX'):
                strx=aggregateColStrip(c[4:])
                val=aggregateFunc(mydict,'max',colmap[strx])
                col_list.append(val)
                print(val,end=" ")
            elif(c[0:3]=='min' or c[0:3]=='MIN'):
                strx=aggregateColStrip(c[4:])
                val=aggregateFunc(mydict,'max',colmap[strx])
                col_list.append(val)
                print(val,end=" ")
            elif(c[0:5]=='count' or c[0:5]=="COUNT"):
                #print("Yes")
                #print(mydict)
                #print(len(mydict))
                if(len(mydict)==0):
                    continue
                col_list.append(len(mydict))
            else:
                col_list.append(mydict[0][colmap[c]])
            #print('')
        prPrint.add_row(col_list)



    else:
        #print(c[0])
        if(column[0]=="distinct" or column[0]=="DISTINCT"):
            column=column[1:len(column)]
            mydata=[]
            for xd in mydict:
                tlist=[]
                #i=0
                for i in column:
                    #print(i,xd)
                    tlist.append(mydict[xd][colmap[i]])
                mydata.append(tlist)
            unique_data = [list(x) for x in set(tuple(x) for x in mydata)]
            for v in unique_data:
                col_list.clear()  
                for z in range(len(column)):
                    print(v[z],end=" ")
                    col_list.append(v[z])
                print("")
                #col_list.append(col_list)
                prPrint.add_row(col_list)
                 

        else:
            #print("asd")
            #print(mydict)
            if gflag==True:
                for c in column:
                    print(mydict[0][colmap[c]],end=" ")
                    col_list.append(mydict[0][colmap[c]])
                prPrint.add_row(col_list)
                #print('')
            else:
                for i in range(count_row):
                    col_list.clear()
                    for c in column:
                        #print(c)
                        print(mydict[i][colmap[c]],end=" ")
                        col_list.append(mydict[i][colmap[c]])
                    print("")
                    prPrint.add_row(col_list)
            #prPrint.add_row(col_list)


def conditionExtractor(Slist):
    i=0
    while(i<len(Slist)):
        if(Slist[i]=='where'):
            break
        i=i+1
    j=i+1
    #print(Slist[j])
    condition=[]
    while (j< len(Slist)):
        condition.append(Slist[j])
        j=j+1
    #print(condition)
    return condition

def groupbyFunc(mydict,col):
    newdict=sorted(mydict.items(), key=lambda item: int(item[1][col]))
    #print(newdict)
    return newdict



def performgroupBy(Slist,mydict,col,column):
    #print(mydict)
    val=mydict[0][1][col]
    i=1
    xtr={}
    xtr[0]=mydict[0][1]
    cnt=1
    while(i < len(mydict)):
        if(mydict[i][1][col]!=val):
            val=mydict[i][1][col]
            #print(xtr)
            xtr=ifrelational(Slist,xtr,column,colmap)
            #print(xtr)
            columnPrinter(xtr,column,colmap,True)
            #print("")
            xtr.clear()
            xtr[0]=mydict[i][1]
            cnt=1
        else:
            xtr[cnt]=mydict[i][1]
            cnt=cnt+1
        i=i+1
    if(len(xtr)>0):
        #print(xtr)
        xtr=ifrelational(Slist,xtr,column,colmap)
        #print(xtr)
        columnPrinter(xtr,column,colmap,True)
        #print("")

def ifrelational(Slist,mydict,column,colmap):
    for i in range(len(Slist)):
        for r in rList:
            if(Slist[i]==r):
                #print("yes",r)
                mydict=performRelational(Slist[i+1],r,mydict,colmap[Slist[i-1]])
    
    return mydict
    


def performRelational(operand,rop,mydict,col):
    cnt=0
    newdict={}
    #print(mydict[0][col])
    #print(len(mydict),operand)
    if(rop=='='):
        #print("yes")
        for i in range(len(mydict)):
            #print(i)
            if( mydict[i][col]==int(operand)):
                newdict[cnt]=mydict[i]
                cnt=cnt+1
        #print(newdict)
        return newdict
    elif(rop=='>='):
        for i in range(len(mydict)):
            if(mydict[i][col]>=int(operand)):
                newdict[cnt]=mydict[i]
                cnt=cnt+1
        return newdict
    elif(rop=="<="):
        for i in range(len(mydict)):
            if(mydict[i][col]<=int(operand)):
                newdict[cnt]=mydict[i]
                cnt=cnt+1
        return newdict
    elif(rop==">"):
        #print(mydict)
        for i in range(len(mydict)):
            if(mydict[i][col]>int(operand)):
                newdict[cnt]=mydict[i]
                cnt=cnt+1
        #print(newdict)
        return newdict
    if(rop=='<'):
        for i in range(len(mydict)):
            if(mydict[i][col]<int(operand)):
                newdict[cnt]=mydict[i]
                cnt=cnt+1
        return newdict

    

def adjustCompile(sstr):
    res=sstr
    n=len(res)
    i=0
    while i<n:
        #print(i,res,n)
        if(res[i]=='>' or res[i]=='<') :
            if(res[i+1]!='='):
                if(res[i-1]!=' ' and res[i+1]!=' '):
                    res=res[0:i]+" "+res[i:len(res)]
                    res=res[0:i+2]+" "+res[i+2:len(res)]
                    n=n+2
                    i=i+2
            else:
                if(res[i-1]!=' ' and res[i+2]!=' '):
                    res=res[0:i]+" "+res[i:len(res)]
                    res=res[0:i+3]+" "+res[i+3:len(res)]
                    n=n+2
                    i=i+2
        elif(res[i]=="="):
            if(res[i-1]!='<' and res[i-1]!='>'):
                if(res[i-1]!=' ' and res[i+1]!=' '):
                    res=res[0:i]+" "+res[i:len(res)]
                    res=res[0:i+2]+" "+res[i+2:len(res)]
                    n=n+2
                    i=i+2
        i=i+1
    #print(res)
    return res



if __name__=='__main__':
    xtr=[]
    col=0
    x=1
    cval=0
    Slist=[]
    sstr=sys.argv[1]
    sstr=adjustCompile(sstr)
    Slist=sstr.split(" ")
    if(Slist[len(Slist)-1]==';'):
        Slist.pop(len(Slist)-1)
    else:
        str=Slist[len(Slist)-1]
        if(str[len(str)-1]==';'):
           # print(str)
            str=str[:len(str)-1]
            #print(str)
            Slist.pop(len(Slist)-1)
            Slist.append(str)
    #print(Slist)
    tables=extractTable(Slist)
    metaList=[]
    with open('metadata.txt','r') as csv_file:
        #csv_reader=txt.reader(csv_file)
        metaList=csv_file.read().split('\n')
    colmap=columnIndexer(metaList,tables)
    frameCreater(tables,len(colmap))
    column=columnExtractor(Slist)
    if(column[0]=='distinct' or column[0]=='DISTINCT'):
        prPrint.field_names=column[1:]
    else:
        prPrint.field_names=column[0:]
    #print(column)
    #print(column)
    #print(colmap)
    #print(df)
    condition=conditionExtractor(Slist)
    i=0
    mydict=[]
    #print(condition)
    
    x=0
    for x in range(len(Slist)-1):
        #print('yes')
        if(Slist[x]=='group' and Slist[x+1]=='by'):
            #print()
            mydict=groupbyFunc(data,colmap[Slist[x+2]])
            performgroupBy(Slist,mydict,colmap[Slist[x+2]],column)

    #mydict=ifrelational(Slist,data,column,colmap)
    #print(Slist)
    #print(mydict)
    #print(mydict[0][1][1])
    #t=mydict[0]
    #print(t[1])
    #print(mydict[0][0])
    #print(data)
    #columnPrinter(mydict,column,colmap,False)
    #print(tables[0])
    #print(data)
    #print(len(data))
    #print(len(tables))
    #alpha=list(df.iloc[3])
    #print(n)
    print(prPrint)
    
    #print(tables) 
#print(list(df.iloc[0]))
#print(alpha)
