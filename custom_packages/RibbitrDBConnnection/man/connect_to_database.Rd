\name{connect_to_database}
\alias{connect_to_database}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{connect_to_database
%%  ~~function to do ... ~~

}
\description{
This Script handles DB connections to the Ribbitr Database

Connect to database runs the code required for connecting to the Ribbitr Database
It will pull from a "secrets" folder which you will need to set the path here!
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
}
\usage{
connect_to_database(x)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
%%     ~~Describe \code{x} here~~
}
}
\details{
Input:
#    file_secrets = String file path to your DB connection secrets CSV
#       Username secrets should be a csv following the format below
#         Description, Secret
#         username,  USERNAME
#         password,  PASSWORD
#         dbname,    DATABASE_NAME
#         host,      HOST URL
#         port,      PORT
Output:
#   ribbitr_connection = Connection the connection to the ribbitr database.
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
##---- Should be DIRECTLY executable !! ----
##-- ==>  Define data, use random,
##--	or do  help(data=index)  for the standard data sets.

## The function is currently defined as
function (x)
{
  }
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory (show via RShowDoc("KEYWORDS")):
% \keyword{ ~kwd1 }
% \keyword{ ~kwd2 }
% Use only one keyword per line.
% For non-standard keywords, use \concept instead of \keyword:
% \concept{ ~cpt1 }
% \concept{ ~cpt2 }
% Use only one concept per line.
