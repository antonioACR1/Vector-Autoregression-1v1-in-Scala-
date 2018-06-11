/*VARMODEL*/

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import breeze.linalg.DenseVector
import breeze.linalg._
import breeze.numerics._
import org.apache.commons.math3.stat.regression.AbstractMultipleLinearRegression
import scala.util.{Try,Success,Failure}

object varModel extends java.io.Serializable {
def lagSelection(df:Array[Array[Double]],maxLag:Int,season:Option[Int]) : (Int,Array[Double])={ 
  val K : Int = df(0).size
  val nObs : Int = df.size
  /*to get labels for each variable*/
  /*drop first maxLag observations, convert to dense matrix. These are the target variables when doing a loop of linear regressions*/
  val yendogArr=df.drop(maxLag)
  val yendog=DenseMatrix(yendogArr.map(_.toArray):_*) 
  /*construct the matrix of shifts of lag equal to maxLag+1. Each shift will have length nObs-lag+1. Then convert to dense matrix */
  val lag : Int =maxLag + 1
  val ylaggedFull=fixedlagMatTrimBothArr(df,lag)
  val ylaggedDM=DenseMatrix(ylaggedFull.map(_.toArray):_*) /*embed in R*/
  /*prepare the dataset for the linear regressions. It will be either 1 column per variable, 2 columns per variable,e tc. which are taken from the yLagged to be defined */
  val ylaggedFullColIndexes=(0 to ylaggedFull(0).size-1).toList
  val yLagged=ylaggedDM(::,ylaggedFullColIndexes.diff(jumping(ylaggedFullColIndexes.size,0,K))) /*ylagged in R*/

  /*number of observations in ylaggedFull (sample is equal to the shift size)*/
  val sample : Double =ylaggedFull.size.toDouble

  /*rhs construction*/
  val intercept=DenseMatrix(Array.fill(sample.toInt)(1.0))
  val trend=DenseMatrix(((maxLag+1) to (maxLag+sample.toInt) by 1).toArray.map(_.toDouble))
  var rhs=DenseMatrix.horzcat(intercept.t,trend.t)

  if(season.isDefined){
    val seasonValue=season.get
    val a=diag(DenseVector.fill(seasonValue){1.0})
    val b=new DenseMatrix(seasonValue,seasonValue,DenseVector.fill(seasonValue*seasonValue){1.0}.toArray)*(1/seasonValue.toDouble)
    val c=a-b
    var cVar=c
    while(cVar(::,0).size <= sample){
    cVar=DenseMatrix.vertcat(cVar,c);
    }
    cVar=cVar(0 to (sample.toInt-1),0 to seasonValue-2)
    rhs=DenseMatrix.horzcat(rhs,cVar)
  }

  /*cache, persist,broadcast the variable yLaggedDM??*/
  val yLaggedDM=DenseMatrix(MatrixUtil.matToRowArrs(yLagged).map(_.toArray):_*) /*ylagged as dense matrix (yLagged is a sliced dense matrix)*/

  val yLaggedArr=MatrixUtil.matToRowArrs(yLagged) /*yLagged as array of arrays*/
  val detint : Int = rhs.cols

   /*Akaike criteria*/
  val sizeJump : Int =yLaggedArr(0).size
  var AICArr=Array[Double]()
  /*loop of regressions*/
  for (i<-0 to maxLag-1){
    var mVar=Array[Array[Double]]()
      for(j<-0 to K-1){
        val regression=new OLSMultipleLinearRegression()
        regression.setNoIntercept(true)
        /*target variable*/
        val y=yendog(::,j).toArray
        /*REVIEW, two possible exceptions come from 1) when transpose of X times X is not invertible, and 2) X has more columns than observations*/
        /*construction of explanatory variables (a concatenation of incompleteX (1 column per variable or 2 columns per variable or 3 columsn per variable or etc) and rhs)*/
        val colIndexCase=jumping(sizeJump,i,K)
        val incompleteX=MatrixUtil.matToRowArrs(yLaggedDM(::,colIndexCase))
        val incompleteXDM=DenseMatrix(incompleteX.map(_.toArray):_*)
        /*concatenate with rhs to get the full explanatory variables*/
        val almostX=DenseMatrix.horzcat(incompleteXDM,rhs)
        val X=MatrixUtil.matToRowArrs(almostX)
        /*apply regression model to get errors*/
        val params=Try{regression.newSampleData(y,X)} match {case
          Success(_)=>regression.estimateResiduals() /*this should be Array[Double]*/
          case Failure(_)=>Array.fill(X(0).size)(0.0)
        }
        /*matrix creation*/
        mVar=mVar:+params
      } 
    val mCoeff=DenseMatrix(mVar.map(_.toArray):_*)
    /*covariance matrix*/
    val detSigma=det((mCoeff*mCoeff.t):*(1/sample)) /*this value depends on mCoeff*/
    val iValue=log(detSigma)+(2/sample)*((i+1)*K*K+K*detint)
    AICArr=AICArr:+iValue  
  } /*end of i's loop*/
  val selectedLagLong=AICArr.zipWithIndex.min._2
  /*REVIEW*/
  val selectedLag=selectedLagLong.toInt
  (selectedLag,AICArr)
}

def fit(df:Array[Array[Double]],maxLag:Int,season:Option[Int]):(breeze.linalg.DenseMatrix[Double],Int)={
val K : Int =df(0).size
val nObs : Int =df.size
val p=lagSelection(df,maxLag,season:Option[Int])._1+1
/*get labels for each variable*/
val yendogArr=df.drop(p)
val yendog=DenseMatrix(yendogArr.map(_.toArray):_*) /*this yendog corresponds to yend in R*/ /**/
val lag=p+1
val ylaggedFull=fixedlagMatTrimBothArr(df,lag)
/*construct the dataset X for the linear regressions. It is p columns per variable etc. which are taken from the yLagged to be defined below*/
val ylaggedDM=DenseMatrix(ylaggedFull.map(_.toArray):_*)
/*REVIEW*/
val ylaggedFullColIndexes=(0 to ylaggedFull(0).size-1).toList
val yLagged=ylaggedDM(::,ylaggedFullColIndexes.diff(jumping(ylaggedFullColIndexes.size,0,K))) /*ylagged in R*/

val sample : Int =ylaggedFull.size
/*rhs*/
val intercept=DenseMatrix(List.fill(sample)(1.0))
val trend=DenseMatrix(((p+1) to (p+sample) by 1).toArray.map(_.toDouble))
var rhs=DenseMatrix.horzcat(intercept.t,trend.t)

if(season.isDefined){
val seasonValue=season.get	
val a=diag(DenseVector.fill(seasonValue){1.0})
val b=new DenseMatrix(seasonValue,seasonValue,DenseVector.fill(seasonValue*seasonValue){1.0}.toArray)*(1/seasonValue.toDouble)
val c=a-b
var cVar=c
while(cVar(::,0).size <= sample){
cVar=DenseMatrix.vertcat(cVar,c);
}
cVar=cVar(p to (df.size-1),0 to seasonValue-2)
rhs=DenseMatrix.horzcat(rhs,cVar)
}

val yLaggedDM=DenseMatrix(MatrixUtil.matToRowArrs(yLagged).map(_.toArray):_*)

var mVar=Array[Array[Double]]() 

for(j<-0 to K-1){
val regression=new OLSMultipleLinearRegression()
regression.setNoIntercept(true)
val y=yendog(::,j).toArray
val almostX=DenseMatrix.horzcat(yLaggedDM,rhs)
val X=MatrixUtil.matToRowArrs(almostX)

regression.newSampleData(y,X)
val params=regression.estimateRegressionParameters() 
/*matrix creation*/
mVar=mVar:+params
} /*end of j's loop*/

val coeff=DenseMatrix(mVar.map(_.toArray):_*)
(coeff,p)
} 

def predict(coeff:breeze.linalg.DenseMatrix[Double],nAhead:Int,df:Array[Array[Double]],p:Int,season:Option[Int]):breeze.linalg.DenseMatrix[Double]={
/*construction of Zdet*/
val K : Int =df(0).size
val trdstart : Int=df.size + 1
val intercept=DenseMatrix(List.fill(nAhead)(1.0))
val trend=DenseMatrix(((trdstart) to (trdstart+nAhead-1) by 1).toArray.map(_.toDouble))
var ZdetDM=DenseMatrix.horzcat(intercept.t,trend.t)
var ZdetArr=MatrixUtil.matToRowArrs(ZdetDM)

if(season.isDefined){
val seasonValue=season.get	
val a=diag(DenseVector.fill(seasonValue){1.0})
val b=new DenseMatrix(seasonValue,seasonValue,DenseVector.fill(seasonValue*seasonValue){1.0}.toArray)*(1/seasonValue.toDouble)
val c=a-b
var cVar=c
val shiftLength=df.size-p
while(cVar(::,0).size <= shiftLength){
cVar=DenseMatrix.vertcat(cVar,c);
}
cVar=cVar(p to (df.size-1),0 to seasonValue-2)
val cycle=cVar(cVar.rows-seasonValue to cVar.rows-1,::)
var seasonal=cycle
while(seasonal.rows<=nAhead){
seasonal=DenseMatrix.vertcat(seasonal,cycle)
}
seasonal=seasonal(0 to nAhead-1,::)
ZdetDM=DenseMatrix.horzcat(ZdetDM,seasonal)
ZdetArr=MatrixUtil.matToRowArrs(ZdetDM)
}

/*prepare first element for the loop*/
var predictions=Array[Array[Double]]()
var lasty : Array[Double] =Lasty(df,p)

/*prepare first dfArr*/
var dfArr= df

/*predictions using a loop*/
for (i<-0 to (nAhead-1)){
val ZArr=lasty++ZdetArr(i)
val Z=DenseMatrix(ZArr)
val forecastDM=coeff*Z.t
val forecastArr=MatrixUtil.matToRowArrs(forecastDM.t)(0)
predictions=predictions :+ forecastArr
dfArr=unionOriginalAndForecast(dfArr,forecastArr)
lasty=Lasty(dfArr,p)
}
val predictionDM=DenseMatrix(predictions.map(_.toArray):_*)
(predictionDM)
}

def getErrors(coeff:breeze.linalg.DenseMatrix[Double],df:Array[Array[Double]],p:Int,season:Option[Int]):breeze.linalg.DenseMatrix[Double]={
/*construction of Zdet*/
val K=df(0).size
var detint : Int = 2
val trdstart : Int =p + 1
val nAhead=df.size-p
val intercept=DenseMatrix(List.fill(nAhead)(1.0))
val trend=DenseMatrix(((trdstart) to (trdstart+nAhead-1) by 1).toArray.map(_.toDouble))
var ZdetDM=DenseMatrix.horzcat(intercept.t,trend.t)
var ZdetArr=MatrixUtil.matToRowArrs(ZdetDM)

if(season.isDefined){
val seasonValue=season.get	
detint = detint + seasonValue - 1
val a=diag(DenseVector.fill(seasonValue){1.0})
val b=new DenseMatrix(seasonValue,seasonValue,DenseVector.fill(seasonValue*seasonValue){1.0}.toArray)*(1/seasonValue.toDouble)
val c=a-b
var cVar=c
val shiftLength=df.size-p
while(cVar(::,0).size <= shiftLength){
cVar=DenseMatrix.vertcat(cVar,c);
}
cVar=cVar(p to (df.size-1),0 to seasonValue-2)
ZdetDM=DenseMatrix.horzcat(ZdetDM,cVar)
ZdetArr=MatrixUtil.matToRowArrs(ZdetDM)
}

var predictions=Array[Array[Double]]()
var lasty : Array[Double] =Lasty(df.take(p),p)
/*prepare first dfArr*/
var dfArr= df.take(p)

/*predictions using a loop*/
for (i<-0 to (nAhead-1)){
val ZArr=lasty++ZdetArr(i)
val Z=DenseMatrix(ZArr)
val forecastDM=coeff*Z.t
val forecastArr=MatrixUtil.matToRowArrs(forecastDM.t)(0)
predictions=predictions :+ forecastArr
dfArr=df.take(p+i+1)
lasty=Lasty(dfArr,p)
}
/*getting errors*/
val actualValues=df.drop(p)
var errors=Array[Array[Double]]()
for (c<-0 to K-1){
val a=predictions.map(x=>x(c))
val b=actualValues.map(x=>x(c))
val error=a.zip(b).map {case (x,y)=>y-x}
errors=errors:+error
}
val errorsDM=DenseMatrix(errors.map(_.toArray):_*)
val residuals=errorsDM.t
residuals
}

/*COMPLEMENTARY FUNCTIONS*/

def jumping(sizeMat : Int,j:Int,K:Int):List[Int]={
/*the arrays inside df should be non-empty and should have the same size*/
val jump=sizeMat/K
var g=Array[Int]()
for (i<-0 to K-1){val s=(jump*i to jump*i+j).toList
g=g++s
}
val g1=g.toList
g1
}

def fixedlagMatTrimBothArr(dfArrCollect:Array[Array[Double]],lag:Int):Array[Array[Double]]={
val incompleteShifts=Lag.lagMatTrimBoth(dfArrCollect,lag)
val K : Int =dfArrCollect(0).size
val nShifts : Int =(incompleteShifts(0).size.toInt/K).toInt
val lastRow=dfArrCollect.drop(dfArrCollect.size-nShifts)
val lastRowArr= lastRow.map{r=>val
array=r.toSeq.toArray
array.map(_.asInstanceOf[Double])
}
val lastRowDM=DenseMatrix(lastRowArr.map(_.toArray):_*)
var lastArr=Array[Double]()
for (i<-0 to lastRow(0).size-1){
val col=lastRowDM(::,i).toArray
/*change the following lines by (0 to col.size-1).map(i=>col.size-1-i).map(i=>col(i))*/
var colVar=Array[Double]() 
for (j<-0 to col.size-1){
colVar=colVar:+col(col.size-1-j)
}
lastArr=lastArr++colVar 
}
val completeShifts=incompleteShifts:+lastArr
completeShifts
}

def unionOriginalAndForecast(dfArr:Array[Array[Double]],forecast:Array[Double]):Array[Array[Double]]={
val dfArrayUnion=dfArr:+forecast
dfArrayUnion
}

def Lasty(dfArr:Array[Array[Double]],p:Int):Array[Double]={
val K=dfArr(0).size
var lasty=Array[Double]()
for(i<-0 to K-1){
for(j<-1 to p){
lasty=lasty:+dfArr(dfArr.size-j)(i)
}
}
lasty
}

}

