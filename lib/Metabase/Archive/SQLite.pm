use 5.006;
use strict;
use warnings;

package Metabase::Archive::SQLite;
# ABSTRACT: Metabase storage using SQLite
# VERSION

use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Types::Path::Class;
use Path::Class ();

use Metabase::Fact;
use Carp        ();
use Data::GUID  ();
use Data::Stream::Bulk::DBIC ();
use JSON 2      ();
use DBI         1 ();
use DBD::SQLite 1 ();
use Compress::Zlib 2 qw(compress uncompress);
use SQL::Translator 0.11006 (); # required for deploy()
use Metabase::Archive::Schema;

with 'Metabase::Archive';

has 'filename' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    coerce   => 1,
    required => 1,
);

has 'compressed' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 1,
);

has 'synchronous' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

has 'schema' => (
    is      => 'ro',
    isa     => 'Metabase::Archive::Schema',
    lazy    => 1,
    default => sub {
        my $self     = shift;
        my $filename = $self->filename;
        my $exists   = -f $filename;
        my $schema   = Metabase::Archive::Schema->connect(
            "dbi:SQLite:$filename",
            "", "",
            {   RaiseError => 1,
                AutoCommit => 1,
            },
        );
        return $schema;
    },
);

sub initialize {
  my ($self, @fact_classes) = @_;
  $self->schema->deploy unless -e $self->filename;
  $self->schema->storage->dbh_do(
    sub {
      my ($storage,$dbh) = @_;
      my $toggle = $self->synchronous ? "ON" : "OFF";
      $dbh->do("PRAGMA synchronous = $toggle");
    }
  );
  return;
}

# given fact, store it and return guid; return
# XXX can we store a fact with a GUID already?  Replaces?  Or error?
# here assign only if no GUID already
sub store {
    my ( $self, $fact_struct ) = @_;
    my $guid = lc $fact_struct->{metadata}{core}{guid};
    my $type = $fact_struct->{metadata}{core}{type};

    unless ($guid) {
        Carp::confess "Can't store: no GUID set for fact\n";
    }

    my $content = $fact_struct->{content};
    my $json    = eval { JSON->new->ascii->encode($fact_struct->{metadata}{core}) };
    Carp::confess "Couldn't convert to JSON: $@"
      unless $json;

    if ( $self->compressed ) {
        $json    = compress($json);
        $content = compress($content);
    }

    $self->schema->resultset('Fact')->create(
        {   guid    => $guid,
            type    => $type,
            meta    => $json,
            content => $content,
        }
    );

    return $guid;
}

# given guid, retrieve it and return it
# type is directory path
# class isa Metabase::Fact::Subclass
sub extract {
    my ( $self, $guid ) = @_;
    my $schema = $self->schema;

    my $fact = $schema->resultset('Fact')->find(lc $guid);
    return undef unless $fact;

    my $type    = $fact->type;
    my $json    = $fact->meta;
    my $content = $fact->content;

    if ( $self->compressed ) {
        $json    = uncompress($json);
        $content = uncompress($content);
    }

    my $meta = JSON->new->ascii->decode($json);

    # reconstruct fact meta and extract type to find the class
    my $class = Metabase::Fact->class_from_type($type);

    return { 
      content => $content, 
      metadata => {
        core => $meta
      },
    };
}

sub delete {
    my ( $self, $guid ) = @_;
    $self->schema->resultset('Fact')->find(lc $guid)->delete;
}

sub iterator {
  my ($self) = @_;
  return Data::Stream::Bulk::DBIC->new(
    resultset => scalar($self->schema->resultset("Fact")->search_rs)
  );
}

1;

__END__

=for Pod::Coverage::TrustPod store extract delete iterator initialize

=head1 SYNOPSIS

  require Metabase::Archive::SQLite;

  $archive = Metabase::Archive::SQLite->new(
    filename => $sqlite_file,
  ); 

=head1 DESCRIPTION

Store facts in a SQLite database.

=head1 USAGE

See L<Metabase::Archive> and L<Metabase::Librarian>.

TODO: document optional C<compressed> option (default 1), C<synchronized>
option and C<schema> option (sensible default provided).

=cut
