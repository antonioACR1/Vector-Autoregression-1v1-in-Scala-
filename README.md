# About

## This is my implementation of Multivariate Vector Autoregression which I used as part of a project to forecast Mexican inflation. This version runs on spark-shell (Spark >= 1.6 ) and you have to make the approprite JAR conversion if you want to use it on cluster mode.

## General

This implementation is based on the R implementation of Vector Autoregression (VAR). Notice, however, that some options from the R implementation are not available here, for example the exogenous option is not available. Also, both intercept and trend are included, so it's not possible to choose either one of them or none. The lag selection is based only on Akaike criteria, so it's not possible to choose another criteria. Furthermore, the forecast does not include confidence levels.

### In case someone is interested, I'm currently working on an update which I expect to finish by (the end of!) December 2018. This will include the exogenous option, the remaining criterias and the confidence levels. This update will be given as a class instead of an object too. 

## Usage

This implementation is the equivalent of the following line in R:

`model<-VAR(y,type="both",season,exogen=NULL,lag.max,ic="AIC")`

and

`predict(VAR(model,n.ahead))`

It is possible to use the parameter `p` instead of `lag.max` by using directly the `fit` method.

### Example

In the spark-shell, run the files in the required libraries folder (first Matrixutil and then Lag). Then run the file in the Vector Autoregression folder.

Once you have done this, your time series should be in the format `Array[Array[Double]]`, for example:

`val df = Array(Array(1.0,11.0),Array(2.0,12.0),Array(3.0,13.0),Array(4.0,14.0),Array(5.0,15.0),Array(6.0,16.0),Array(7.0,17.0),Array(8.0,18.0),Array(9.0,19.0),Array(10.0,20.0))`

Set the maximum lag and the season 

`val maxLag=2` 

`val season=Some(2)`

In case you don't want to use season, set

`season=None`

Now to get the appropriate lag, type

`val lagSelectionOutput=varModel.lagSelection(df,maxLag,season)`

To train the model, type

`val model=varModel.fit(df,maxLag,season)`

If you want to use your own `p` instead of the one provided by the Akaike criteria, type instead

`val model=varModel.fit(df,p,season)`

To predict say 12 values ahead, type

`val prediction=varModel.predict(model._1,nAhead=12,df,model._2,season)`

To get the errors of the model, type

`val errors=varModel.getErrors(model._1,df,model._2,season)`
