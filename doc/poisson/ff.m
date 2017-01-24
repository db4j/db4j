# copyright 2017 nqzero - see License.txt for terms

1;

function p = kgivenb(bn,k)
  p = 2*(k+1)./(bn^2).*gammainc( bn, k+2 );
end


# return the inverse transform sampling based on numerically inverting a pdf(x)
#   uniform: either an array of probs to transform or the length of a rand vector to use
#   p: optional, either the probs to interpolate at or the number of points to use
function [prob,cdf,func] = invert(x,pdf,uniform,p=0:.001:1)
  if (isscalar(uniform) & uniform>=1); uniform = rand( uniform, 1 ); endif
  if (isscalar(p)       &       p> 1);       p = 0:1/p:1;            endif
  cdf = cumtrapz( x, pdf );
  cdf /= cdf( end );
  [uc,iic] = unique(cdf,'first');
  prob = interp1( cdf(iic), x(iic), uniform );
  # func = interp1( cdf(iic), x(iic), p );
  # [uf,iif] = unique(p);
  # prob = interp1( p(iif), func(iif), uniform );
end


function prob = pgivenkb(b,n,p,k)
  pn = p*n; bn = b*n;
  g = gammainc( bn, k+2 ) .* gamma( k+2 );
  prob = (pn.^(k+1)).*(e.^-pn) ./ g * n;
  prob( p >= b ) = 0;
end


function prob = phat(a,m,k,n); prob = ( k.*(m+n) + a.*m.^2 ) ./ ( m.^2 + m.*n + n.^2 ); end

function xx = ktau(a,tau,m,n,u)
  xx.k = ktau = ((tau-a)*m.^2 + tau*(m.*n + n.^2)) ./ (m+n);
  x = u.*n;
  # probs that P(p^ > tau), P(p^ == tau), P(p^ >= tau )
  xx.cmf = gammainc( x, ktau+1 );
  xx.pdf = x.^ktau .* e.^-x ./ gamma(ktau+1);
  xx.ptau = xx.cmf + xx.pdf;
end

function prob = poiss(k,nu); prob = nu.^k .* e.^(-nu) ./ gamma( k+1 ); end

function prob = pramp(p,k,n,b); prob = poiss(k+1,n*p) .* n ./ gammainc(n*b,k+2); end

function index = findzero(x); [junk,index] = min(abs(x)); end;

function [ppbk,ppk] = un2(b,c,k,n,p)
  gr1 = gammainc(b*n,  k+1);
  gr2 = gammainc(b*n*2,k+1);
  theta = (1-c)*gr1 + c*gr2;
  ppbk = c * (gr2 - gr1) ./ theta;
  if (nargout >= 2)
    scale = (n./theta).*(p<(2*b));
    scale(p >= b) *= c;
    ppk = scale*poiss(k,n*p);
  end
end






