# copyright 2017 nqzero - see License.txt for terms

1;


function uo = mymean(vals)
  uo = 0;
  if (length(vals) > 0) uo = mean(vals); end
end

function uhist(x);
  y = unique(x);
  hist(x,y);
  axis([y(1)-.5 y(end)+.5]);
end

function hist2(x,y,varargin)
  [gx,gn] = hist(x,varargin{:});
  [hx,hn] = hist(y,varargin{:});
  gh = bar(gn,gx);
  hold on;
  sleep(0);
  dh = hn(end)-hn(1);
  hh = bar(hn+dh*.01,hx,'facecolor','r');
  hc = get(hh,'children');
  set(hc,'facealpha',.5);
  hold off;
end

function nplot(x,y,dx,dy)
  plot( x+dx*rand(size(x)), y+dy.*rand(size(y)), 'x' );
end

function [h1 h2] = bar2(domain,n1,n2)
  h1 = bar(domain,n1); hold on; h2 = bar(domain,n2,'facecolor','r'); hold off;
end

function zo = who2struct(zo=struct,names='')
  if (ischar(names)); names = evalin( 'caller', ['who ' names] ); end
  for ii = 1:length(names)
    zo.(names{ii}) = evalin( 'caller', names{ii} );
  end
end

function names = struct2who(zo,names=fieldnames(zo))
  if (ischar(names)) names = strsplit( names, ' ,' ); end
  for ii = 1:length(names)
    assignin( 'caller', names{ii}, zo.(names{ii}) );
  end
end




function x = genu(z,n,xo=[])
  iu = [];
  extra = .1;
  while (length(iu) < n)
    ne = n + extra*(n+length(xo));
    y = rand( ne, 1 );
    x = ceil(interp1( z.xcdf, z.xmap, y ));
    xc = [x;xo];
    [xu,iu,ju] = unique( xc );
    iu = sort(iu( iu < ne ));
    extra *= 2;
  end
  x = xc(iu(1:n));
end

% for each value in the domain, count the occurrences of the value (vt: total), and the value conditioned on cond (vg:good)
% doplot --> display a histogram of the counts over the domain
function [vg vt domain] = countOccur(vals,domain=unique(vals),cond=1,doplot=0)
  vt = vg = 0*domain;
  for ii = 1:length(vt); val = domain(ii); vg(ii) = sum(vals==val & cond); vt(ii) = sum(vals==val); end;
  if (doplot);
    bar2(domain,vt,vt-vg);
    axis([domain(1),domain(end)]);
    sleep(0);
  end
end


function zo = stepInit()
  nn = 10^6;
  px = sort(rand(1,nn));
#  px = fliplr( px );
  px /= sum(px);
  xcdf = cumsum( px );
  xmap = 1:nn;
  nc = 1000;
  kc = kd = kn = zeros(1,nc);
  po = 2/nn;
  nq = 1/po;
  sat = 16;
  satk = sat / 16; satk = 1;
  skip = 1;
  dbins = 6;
  uhat = 0;
  iigen = 0;
  zo = who2struct();
  zo.xa = genu( zo, nc );
end



function dispCounts(z)
  struct2who(z,'kn sat satk skip llgood iigen');
  if (mod(iigen,skip)==0); countOccur(floor(kn/satk),0:sat/satk,llgood,1); end
end

function vr = unbiasedRound(vals)
  vr = round(vals);
  num = sum(vals-vr);
  delta = 1;
  if (num<0); delta = -1; num = -num; end
  kk = ceil( length(vals) * rand(round(num),1) );
  # fixme::precision -- should really offset the num most in need of it, instead of randoms
  #  or at the least, only offset in the correct direction
  vr(kk) += delta;
end

function [kc,kn] = saturateCounts(kc,kn,sat)
  ks = kn>=sat;
  if (length(sat)>1) sat = sat(ks); end
  kf = kc(ks) .* (sat-1) ./ kn(ks);
  kr = unbiasedRound(kf);
  kc(ks) = round(kr);
  kn(ks) = sat-1;
end


function z = updateVectors(z)
  names = 'nc xa kc kn kd llgood';
  struct2who(z,names);
  n4 = nc - sum(llgood);
  kw = 1:n4;
  ko = n4+1:nc;
  xa(ko) = xa(llgood);
  kn(ko) = kn(llgood);
  kc(ko) = kc(llgood);
  kd(ko) = kd(llgood);
  if (n4>0)
    xa(kw) = genu( z, n4, xa(ko) ); kn(kw) = 0; kc(kw) = 0; dk(kw) = 0;
  end
  z = who2struct(z,names);
end


function z = updateLoop(z,final)
  struct2who(z,'nq px xa kc kn kd po nn uhat dbins iigen');
  k = poissrnd( nq * px(xa) );
  babies = mymean(kn==0);
  msg = "";
  if (babies > .01 & babies < .99)
    if (mymean(k(kn==0)) > .1+mymean(k(kn>0)))
      msg = sprintf( "-- babies(%8.3f,%8.3f)", mymean(k(kn==0)), mymean(k(kn>0)) );
      kc(:) = 0; kn(:) = 0; kd(:) = 0;
    end
  end
  kd++;
  kd(k>0) = 0;
  kc += k;
  kn++;
  
  praw = kc./kn/nq;
  llgood = praw + 1*sqrt( po./kn/nq ) > po;
  llgood &= (kd<dbins);
  if (0)
    po = 2e-6;
    dev = sqrt( po./kn/nq );
    scaled = (praw-po) ./ dev;
    thresh = quantile( scaled', .1 );
    llgood = scaled > thresh;
  end
  gain = .5;
  uhat = gain*uhat + (1-gain)*mymean(k(kn>0));
  iigen++;
  z = who2struct(z,'k kc kd kn praw llgood uhat iigen');
  ss = stats(z);
  struct2who(ss);
  printf( "%5d %8.3f %8.3f %8.3f %5d %5d %s\n", iigen, ktarg, ktrue, klive, nsurv, nhits, msg );
  z.ss(iigen) = ss;
end

function z = calcThresh(z)
  struct2who(z,'kc kn praw nn');
  isb = kn==1;
  nb = sum(isb);
  nc = length(isb) - nb;
  nm = 100;
  broom = cache = 0;
  if (nb > nm); broom = quantile( praw(isb)', .5 ); end
  if (nc > nm); cache = quantile( praw(!isb)', .2 ); end
  z.po = max(broom,cache);
  z.po = quantile( praw', .5 );
end

function z = prepLoop(z)
  z = calcThresh(z);
  [z.kc,z.kn] = saturateCounts(z.kc,z.kn,z.sat);
  dispCounts(z);
  z = updateVectors(z);
#  z.po = quantile( z.praw', .2 );
end

function ret = stats(z)
  names = struct2who(z,'llgood px xa po kn k nq');
  names = who();
  ktarg = nq*po;                   % target hit rate
  ktrue = nq*mymean(px(xa));
  kgood = nq*mymean(px(xa(llgood))); % "true" hit rate for the survivors
  klive = nq*mymean(k(kn>1))/nq;     % hit rate for the adult gen
  ktotal= nq*mymean(k)/nq;
  nhits = sum(k);
  nadult = sum(kn>1);
  ngood = sum(llgood);
  nsurv = sum(llgood & (kn>1));    % number of adult survivors
  clear(names{:});
  ret = who2struct();
end





function z = loop(z,ng)
  for ii = 1:ng
    z = updateLoop(z,0);
    z = prepLoop(z);
  end
end

function z = flip(z)
  z.px = fliplr(z.px);
  z.xcdf = cumsum( z.px );
end

function z = demo()
  z = stepInit();
  z = loop(z,100);
end






