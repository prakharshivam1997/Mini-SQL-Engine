import csv
import sys
#from prettytable import PrettyTable
data={}
finalList=[]
#prPrint=PrettyTable()                         #prettyTable-----------------------------------------------------------
rList=['=','>','<','>=','<=']
allColumns=[]
alltables=[]
oflag=False
def extractTable(Slist):      
    i=1
    tables=[]
    while(i<len(Slist)):
        if(Slist[i]=="from"):
            break
        i=i+1
    i=i+1
    while i<len(Slist):
        if(Slist[i]=='where' or Slist[i]=="group" or Slist[i]=="order"):
            break
        else:
            if(Slist[i]!="," and Slist[i]!=''):
                tables.append(Slist[i])
        i=i+1
    e_length=0
    for t in tables:
        if(t!='' and t!=" "):
            e_length=e_length+1

    if(e_length==1):
        tables=tables[0].split(',')
        return tables
    else:
        newtables=[]
        for z in tables:
            if(z!="" and z!=" "):
                if(z[-1]==','):
                    newtables.append(z[0:-1])
                else:
                    newtables.append(z)
        
        return newtables

def alltableCreate(metaList):
    i=0
    try:
        for i in range(len(metaList)):
            if(metaList[i]=="<begin_table>"):
                alltables.append(metaList[i+1])
    except:
        print("Error in metatable,exiting :(")
        exit()    


def tableChecker(tables):
    for t in tables:
        flag=False
        for z in alltables:
            if(z==t):
                flag=True
        if(flag==False):
            print("error in table name,does not match with metaTable,exiting :(")
            exit()

def columnSlicer(str_now):
    if(str_now[0:3]=='sum' or str_now=="SUM"):
        return str_now[4:-1]
    if(str_now[0:3]=='min' or str_now=="MIN"):
        return str_now[4:-1]
    if(str_now[0:3]=='max' or str_now=="MAX"):
        return str_now[4:-1]
    if(str_now[0:3]=='avg' or str_now=="AVG"):
        return str_now[4:-1]
    if(str_now[0:5]=="count" or str_now[0:5]=="COUNT"):
        return str_now[6:-1]
    else:
        return str_now


def columnChecker(column):
    for c in column:
        flag=False
        if(c=="distinct" or c=="DISTINCT"):
            continue
        for t in allColumns:
            str_now=c
            str_now=columnSlicer(str_now)
            if(str_now==t or str_now=="*" ):
                flag=True
        if(flag==False):
            print("Error in column names, not matching with Meta Table,exiting :(")
            exit()


def valueAppender(q1,q2):
    for i in range(len(q2)):
        if(isinstance(q2[i],float)):
            q1.append(q2[i])
        else:
            q1.append(int(q2[i]))




def valueEraser(temp,line):
    i=0
    for i in range(len(line)):
        temp.pop()

def tableSizeMap(tables,sizeMap):
    x=0
    col_size=0
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
    tableSizeMap(tables,sizeMap)
    frameList(tables,0,xtr,temp,sizeMap,cval)



def columnIndexer(metaList,tables):                                # creates dictionary with column values and index
    x=0
    colmap={}
    i=0
    
    for t in tables:
        i=0
        while i<len(metaList):
            z=i
            if(metaList[i]=='<begin_table>'):
                if(t==metaList[i+1]):
                    z=i+2
                    while(z<len(metaList) and metaList[z]!='<end_table>'):
                        colmap[metaList[z]]=x
                        allColumns.append(metaList[z])
                        x=x+1
                        z=z+1
            i=z+1
    return colmap
    


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
    if(len(column)==1 and column[0]=="*"):
        for z in allColumns:
            result.append(z)
        return result
    else:
        if(len(column)>1):
            for x in column:
                temp=x.split(',')
                for t in temp:
                    if(t!='' and t!=' '):
                        result.append(t)
            return result
        else:
            return column[0].split(',')
    return result




def aggregateFunc(mydict,type,col):                  #calculates value of aggregate functions
    count_row=len(mydict)
    sum=float(0)
    i=0
    if(count_row==0):
        print("No value in data frame")
        exit()
    if(type=='sum'):
        while i<count_row:
            #print(mydict[i][col])            #remove it
            sum=sum+mydict[i][col]
            i=i+1
        return sum
    elif(type=='AVG' or type=='avg'):
        n=0
        sum=float(0)
        i=0
        while i<count_row:
            sum=float(sum+mydict[i][col])
            n=n+1
            i=i+1
        return float(sum/float(n))
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
            i=i+1
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
    col_list=[]
    if(isAggregateFunc(column)==True):
       
        try:
            for c in column:
                if(c[0:3]=='sum'):
                    strx=aggregateColStrip(c[4:])
                    try:
                        val=aggregateFunc(mydict,'sum',colmap[strx])
                        col_list.append(val)
                    except:
                        print("Error in column name,exiting :(")
                        exit()
                elif(c[0:3]=='avg' or c[0:3]=='AVG'):
                    strx=aggregateColStrip(c[4:])
                    try:
                        val=aggregateFunc(mydict,'avg',colmap[strx])
                        col_list.append(val)
                    except:
                        print("Error in column name,exiting :(")
                        exit()
                elif(c[0:3]=='max' or c[0:3]=='MAX'):
                    strx=aggregateColStrip(c[4:])
                    try:
                        val=aggregateFunc(mydict,'max',colmap[strx])
                        col_list.append(val)
                    except:
                        print("Error in column name,exiting :(")
                        exit()
                elif(c[0:3]=='min' or c[0:3]=='MIN'):
                    strx=aggregateColStrip(c[4:])
                    try:
                        val=aggregateFunc(mydict,'min',colmap[strx])
                        col_list.append(val)
                    except:
                        print("Error in column name,exiting :(")
                        exit()
                elif(c[0:5]=='count' or c[0:5]=="COUNT"):
                    try:
                        if(len(mydict)==0):
                            continue
                        col_list.append(len(mydict))
                    except:
                        print("Error in count function,exiting :(")
                        exit()
                else:
                    try:
                        col_list.append(mydict[0][colmap[c]])
                    except:
                        print("Error in column name,exiting :(")
                        exit()
                #print('')
            tempx=[]
            valueAppender(tempx,col_list)
            finalList.append(tempx)
            #prPrint.add_row(col_list)                           # pretty table ---------------------------------
        except:
            print("error in query formatting,exiting, :(")
            exit()



    else:
        if(column[0]=="distinct" or column[0]=="DISTINCT"):
            column=column[1:len(column)]
            mydata=[]
            for xd in mydict:
                tlist=[]
                for i in column:
                    tlist.append(mydict[xd][colmap[i]])
                mydata.append(tlist)
            unique_data = [list(x) for x in set(tuple(x) for x in mydata)]
            for v in unique_data:
                col_list.clear()  
                for z in range(len(column)):
                    col_list.append(v[z])
                tempx=[]
                valueAppender(tempx,col_list)
                finalList.append(tempx)
                #prPrint.add_row(col_list)            # pretty table---------------------------------------------
                 

        else:
            if gflag==True:
                for c in column:
                    col_list.append(mydict[0][colmap[c]])
                tempx=[]
                valueAppender(tempx,col_list)
                finalList.append(tempx)
                #prPrint.add_row(col_list)           # prettyTable-----------------------------------------------
            else:
                cnt=0
                newdict={}
                for key in mydict:
                    newdict[cnt]=mydict[key]
                    cnt=cnt+1
                #print(newdict)
                for i in range(len(newdict)):
                    col_list.clear()
                    for c in column:
                        col_list.append(newdict[i][colmap[c]])
                    #print("")
                    temp=[]
                    valueAppender(temp,col_list)
                    finalList.append(temp)
                    #prPrint.add_row(col_list)               # prettyTable------------------------------------
            


def conditionExtractor(Slist):
    i=0
    while(i<len(Slist)):
        if(Slist[i]=='where'):
            break
        i=i+1
    j=i+1
   
    condition=[]
    while (j< len(Slist)):
        condition.append(Slist[j])
        j=j+1
    
    return condition


def dictAppend(newdict1,newdict2):
    cnt=0
    newdictx={}
    for key in newdict1:
        newdictx[cnt]=newdict1[key]
        cnt=cnt+1
    for key in newdict2:
        newdictx[cnt]=newdict2[key]
        cnt=cnt+1
    #print(newdict1)
    return newdictx


def ifIsOr(Slist,mydict,colmap):
    i=0
    while i<len(Slist):
        if(Slist[i]=="AND" or Slist[i]=="and"):
            try:
                #print(Slist[i-1])
                mydict=performRelational(Slist[i-1],Slist[i-2],mydict,colmap[Slist[i-3]],colmap)
                newdictx={}
                newdictx=dictAppend(newdictx,mydict)
                mydict=performRelational(Slist[i+3],Slist[i+2],newdictx,colmap[Slist[i+1]],colmap)
            except:
                print("Error in column name,exiting")
                exit()
          
            return newdictx
        elif(Slist[i]=="OR" or Slist[i]=="or"):
            try:
                newdict1={}
               
                newdict1=performRelational(Slist[i-1],Slist[i-2],mydict,colmap[Slist[i-3]],colmap)
                newdict2={}
                newdict2=performRelational(Slist[i+3],Slist[i+2],mydict,colmap[Slist[i+1]],colmap)
                if(len(newdict1)>len(newdict2)):
                    return dictAppend(newdict1,newdict2)
                else:
                    return dictAppend(newdict1,newdict2)
            except:
                print("Error in column name111, exiting")
                exit()
        i=i+1
    return mydict


def groupbyFunc(mydict,col):
    newdict=sorted(mydict.items(), key=lambda item: int(item[1][col]))
    return newdict


def performgroupBy(Slist,mydict,col,column):
    #print(mydict)
    val=mydict[0][col]
    i=1
    xtr={}
    xtr[0]=mydict[0]
    cnt=1
    while(i < len(mydict)):
        if(mydict[i][col]!=val):
            val=mydict[i][col]
            
            xtr=ifrelational(Slist,xtr,column,colmap)
            columnPrinter(xtr,column,colmap,True)
            xtr.clear()
            xtr[0]=mydict[i]
            cnt=1
        else:
            xtr[cnt]=mydict[i]
            cnt=cnt+1
        i=i+1
    if(len(xtr)>0):
        xtr=ifrelational(Slist,xtr,column,colmap)
        columnPrinter(xtr,column,colmap,True)


def ifrelational(Slist,mydict,column,colmap):
    for i in range(len(Slist)):
        for r in rList:
            if(Slist[i]==r):
                mydict=performRelational(Slist[i+1],r,mydict,colmap[Slist[i-1]],colmap)
    
    return mydict
    


def performRelational(operand,rop,mydict,col,colmap):
    #print("Ene")
    oflag=False
    for x in allColumns:
        if(x==operand):
            oflag=True
    cnt=0
    #print(oflag)
    newdict={}
    if(rop=='='):
        try:
            for i in range(len(mydict)):
                if(oflag==True):
                    if(mydict[i][col]==mydict[i][colmap[operand]]):
                        newdict[cnt]=mydict[i]
                else:
                    if( mydict[i][col]==int(operand)):
                        newdict[cnt]=mydict[i]
                cnt=cnt+1
        except:
            print("Error in operand supplied.1 :(")
            exit()
        return newdict
    elif(rop=='>='):
        try:
            for i in range(len(mydict)):
                if(oflag==True):
                    if(mydict[i][col]>=mydict[i][colmap[operand]]):
                        newdict[cnt]=mydict[i]
                else:
                    if(mydict[i][col]>=int(operand)):
                        newdict[cnt]=mydict[i]
                cnt=cnt+1
        except:
            print("Improper in operand supplied.2 :(")
            exit()
        return newdict
    elif(rop=="<="):
        try:
            for i in range(len(mydict)):
                if(oflag==True):
                    if(mydict[i][col]<=mydict[i][colmap[operand]]):
                        newdict[cnt]=mydict[i]
                else:
                    if(mydict[i][col]<=int(operand)):
                        newdict[cnt]=mydict[i]
                cnt=cnt+1
        except:
            print("Improper in operand supplied.3 :(")
            exit()
        return newdict
    elif(rop==">"):
        #print("ads")
        try:
            #print(len(mydict),col,operand)
            for i in range(len(mydict)):
                if(oflag==True):
                    if(mydict[i][col]>mydict[i][colmap[operand]]):
                        newdict[cnt]=mydict[i]
                else:
                    #print(operand,col,mydict[i][col])
                    if(mydict[i][col]>int(operand)):
                        newdict[cnt]=mydict[i]
                cnt=cnt+1
            #print(newdict)
            #print(col)
        except:
            print("Improper in operand supplied.4 :(")
            exit()
        #print(newdict)
        return newdict
    if(rop=='<'):
        try:
            for i in range(len(mydict)):
                if(oflag==True):
                    if(mydict[i][col]<mydict[i][colmap[operand]]):
                        newdict[cnt]=mydict[i]
                else:
                    if(mydict[i][col]<int(operand)):
                        newdict[cnt]=mydict[i]
                cnt=cnt+1
        except:
            print("Improper in operand supplied.5 :(")
            exit()
        return newdict

    

def adjustCompile(sstr):
    res=sstr
    n=len(res)
    i=0
    while i<n:
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
    return res

def orderbyFunc(Slist,mydict,colmap):
    i=0
    cnt=0
    newdict={}
    flag=False
    while i<len(Slist):
        if(Slist[i]=="order" and Slist[i+1]=="by"):
            try:
                if(Slist[i+3]=="asc" or Slist[i+3]=="ASC"):
                    flag=True
                    col=colmap[Slist[i+2]]
                    ldict=sorted(mydict.items(), key=lambda item: int(item[1][col]))
                if(Slist[i+3]=="desc" or Slist[i+3]=="DESC"):
                    flag=True
                    col=colmap[Slist[i+2]]
                    ldict=sorted(mydict.items(), key=lambda item: int(item[1][col]),reverse=True)
            except:
                print("error in order by, exiting :(")
                exit()
        i=i+1
    if(flag==True):
        oflag=True
        for z in range(len(ldict)):
            newdict[cnt]=ldict[z][1]
            cnt=cnt+1
        return newdict
    else:
        return mydict                      

if __name__=='__main__':
    xtr=[]
    col=0
    x=1
    cval=0
    Slist=[]
    sstr=sys.argv[1]
    sstr=adjustCompile(sstr)
    Slist=sstr.split(" ")
    commaflag=False
    if(Slist[len(Slist)-1]==';'):
        Slist.pop(len(Slist)-1)
        commaflag=True
    else:
        str=Slist[len(Slist)-1]
        if(str[len(str)-1]==';'):
            str=str[:len(str)-1]
            Slist.pop(len(Slist)-1)
            Slist.append(str)
            commaflag=True
    if(commaflag==False):
        print("Error, semicolon not applied at the end, exiting")
        exit()
    tables=extractTable(Slist)
    metaList=[]
    with open('metadata.txt','r') as csv_file:
        metaList=csv_file.read().split('\n')
    alltableCreate(metaList)
    tableChecker(tables)
    colmap=columnIndexer(metaList,tables)
    column=columnExtractor(Slist)
    columnChecker(column)
    frameCreater(tables,len(colmap))
    
    if(column[0]=='distinct' or column[0]=='DISTINCT'):
        temp=[]
        #valueAppender(temp,column[1:])
        for x in column[1:]:
            temp.append(x)
        finalList.append(temp)
        #prPrint.field_names=column[1:]                  #prettyTable-----------------------------------------------
    else:
        temp=[]
        #valueAppender(temp,column[0:])
        for x in column[0:]:
            temp.append(x)
        finalList.append(temp)
        #prPrint.field_names=column[0:]                  #prettyTable------------------------------------------------------
    condition=conditionExtractor(Slist)
    i=0
    mydict={}
    x=0
    gflag=False
    for x in range(len(Slist)-1):
        if(Slist[x]=='group' and Slist[x+1]=='by'):
            #print()
            try:
                gflag=True
                mydict=groupbyFunc(data,colmap[Slist[x+2]])
                cnt=0
                newdict={}
                for m in mydict:
                    newdict[cnt]=m[1]
                    cnt=cnt+1
                    #print(m[1])
                #print(newdict)
                mydict=orderbyFunc(Slist,newdict,colmap)
                performgroupBy(Slist,mydict,colmap[Slist[x+2]],column)
            except:
                print("Error in group by column names, exiting :(")
                exit()
    if(gflag==False):
        isorFlag=False
        for k in Slist:
            if(k=="and" or k=="AND" or k=="OR" or k=="or"):
                mydict=ifIsOr(Slist,data,colmap)
                mydict=orderbyFunc(Slist,mydict,colmap)
                columnPrinter(mydict,column,colmap,False)
                isorFlag=True

        relflag=False
        if(isorFlag==False):
            for i in range(len(Slist)):
                for r in rList:
                    if(Slist[i]==r):
                        relflag=True
                        break
            if(relflag==True):
                mydict=orderbyFunc(Slist,data,colmap)
                mydict=ifrelational(Slist,mydict,column,colmap)
            if(isorFlag==False and relflag==False):
                mydict=orderbyFunc(Slist,data,colmap)
                columnPrinter(mydict,column,colmap,False)
            else:
                mydict=orderbyFunc(Slist,mydict,colmap)
                columnPrinter(mydict,column,colmap,False)
    for f in range(len(finalList)):
        if(f==0):
            print('<',end=" ")
        for q in range(len(finalList[f])):
            if(q==len(finalList[f])-1):
                print(finalList[f][q],end=" ")
            else:
                print(finalList[f][q],end=",")
        if(f==0):
            print('>',end=" ")
        print()
