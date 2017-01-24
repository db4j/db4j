# copyright 2017 nqzero - see License.txt for terms

1;

function A = render(n)
  A = zeros(n,n);
  ki = 0:n-1;
  np = 0:n-1;
  A(1,1) = 1;
  for ii = 2:n; A(:,ii) = poisspdf(ki,np(ii)); end
end



function [mse,chat] = fit(A,c,p)
  nc = length(c);
  np = length(p);
  p = max(p,0);
  p = p/sum(p);
  chat = A(1:nc,1:np)*p(:);
  c2 = chat*0;
  c2(1:nc) = c;
  mse = sqrt( sum((chat-c2(:)).^2) );
end

% solve A*p = c in the least squares sense
function [p,Ao] = deconv(A,c,np)
  nc = length(c);
  Ao = A(1:nc,1:np);
  Ap = pinv(Ao);
  p = Ap*c;
end
