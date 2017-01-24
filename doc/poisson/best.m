# copyright 2017 nqzero - see License.txt for terms

broomx = [ 0:5:145 ];
cachex = [ 70:5:195 ];
cache = [
	 0     1     1     7    19    34    69   108   125   156   162   125    92    50    25    10     5     0     0     0     0     0 \
	 0     0     0     0
	 ];

broom = [
	 1    7   13   14   17   16   23   25   28   31   35   42   41   48   49   58   57   54   54   64   68   56   50   48   39   22 \
	 14   7    4    1
	 ];





zz = []; for ii = 1:length(broom); zo = broomx(ii)*ones( 1, broom(ii) ); zz = [zz,zo]; end;

bv = 100:200;
ss = zz;
ll = []; for bn = bv; aa = kgivenb( bn, ss ); ll(end+1) = sum( log( aa ) ); end;

p = 0:.001:1;
vals = rand( 1000, 1 );

aa = kgivenb( 120, broomx );
bb = cumtrapz( broomx, aa ); bb /= max( bb );
cc = interp1( bb, broomx, p ); plot( p, cc );
dd = interp1( p, cc, vals );

dd = invert( broomx, aa, 1000 );

v = []; for ii = 1:length(kk); k = kk(ii); v(ii) = mean( poisspdf( k, dd ) ); end;
plot( broomx, broom/trapz(broomx,broom), 'x' ); hold on; plot( broomx, aa, 'r' ); hold off;




b = .001; n = 10000; po = [ 0:b/100:b b*1.01:.01:1 ]; pdf = 2*po/(b^2); pdf( po>b ) = 0; pt = invert( po, pdf, 10000 );
kk = poissrnd( pt*n );

pn = 0:.01:1.01*b*n;
kv = 0:15;


ip = [];
clf;
hold on;
for ii = 1:length(kv);
  ki = kk(ii);
  p2 = pgivenkb( b, n, pn/n, ki );
  ppk = pn.^(ki+1).*e.^(-pn) ./ gammainc( b*n, ki+2 ) ./ gamma( ki+2 );
  ppk( pn>=b*n) = 0;
  ppk = p2;
  plot( pn, ppk );
  ip(ii) = trapz( pn, ppk );
  end
  hold off




# plot histograms vs prob( p | k ) for each k ...
for ii = 1:length(kv);
  ki = kv(ii);
  pi = pt(kk==ki);
  figure(ii);
  clf;
  p2 = pgivenkb( b, n, pn/n, ki );
  [nn,xx] = hist( pi );
  bar(xx,nn);
  hold on;
  plot( pn/n, p2*trapz(xx,nn) );
  hold off;
end




