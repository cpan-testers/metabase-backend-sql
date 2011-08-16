package Test::Metabase::StringFact;
use strict;
use warnings;
use parent 'Metabase::Fact::String';

sub content_metadata {
  my $self = shift;
  return {
    'size' => length $self->content,
    'WIDTH' => length $self->content,
  };
}

sub validate_content {
  my $self = shift;
  $self->SUPER::validate_content;
  die __PACKAGE__ . " content length must be greater than zero\n"
  if length $self->content < 0;
}

1;