# snow_melt_hypothesis
Scripts associated with assessing the effect of snow melt on Mountain Yellow Legged Frogs in the Eastern Sierras 

These Scripts are highly dependent on Ned Bair's work from https://github.com/edwardbair/ParBal/blob/master/getMelt.m 
Some of Ned Bairs scripts have been adapted or otherwise used to reanalyze his data and combine it with the Ribbitr data to determine how snow melt in the sierras is associed with Mountain Yellow Legged Frog
population health in the eastern sierras.


The driving theory is that increased melt rates should be associated with increased rates of BD infection.   

The spatial data obtained by Ned Bair is quite large.  So several steps have been taken to reduce these data into chunks managable by an average personal computer.   Furthermore some scripts for analysis are
more easily done in MatLab and other in Python.  For this work I shall be using both languages (and maybe even shome shell scripts).   I apologize to my future self and anyone interested in this work.  Good luck 
and god speed.

====================================================================================================================================================================================================================================

The raw data can be downloaded here:
  500m Data from 2001-2021:
    Bair, E.H. (2023). SPIReS-MODIS-ParBal snow water equivalent reconstruction: Western USA, water years 2001-2021, doi: 10.25349/D9TK7H
    https://datadryad.org/stash/dataset/doi:10.25349/D9TK7H
  
  
  30m Data from 2018-2021:
    May need to ask Ned Bair for this info.   Paper and data discussing this are
    Bair, E.H. (2023), Snow cover and snow water equivalent for “How do tradeoffs in satellite spatial and temporal resolution impact snow water equivalent reconstruction?”, doi: 10.25349/D9PW47.
  
    w/ accompanying paper:
  
    Bair, E. H., J. Dozier, K. Rittger, T. Stillinger, W. Kleiber, and R. E. Davis (2023), How do tradeoffs in satellite spatial and temporal resolution impact snow water equivalent reconstruction?, The Cryosphere, 17, 2629–2643, doi: 10.5194/tc-17-2629-2023.
