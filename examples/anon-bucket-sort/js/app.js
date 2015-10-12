$(document).ready(function(){

    function updateCounts (){
        var totalItems = $('.item').length;
        var leftItems = $('#list .item').length;
        var progress = (totalItems - leftItems)/totalItems * 100;
        $('#itemsLeft').text(leftItems);
        $('#totalItems').text(totalItems);
        $('#itemProgress').css('width', progress + '%').attr('aria-valuenow', progress);
        if(leftItems === 0){
            $('#continueButton').show();
        } else {
            $('#continueButton').hide();
        }
    }

    function updateCard(card, bucket) {
        $.ajax({
            method: !!$(card).data('doc-id') ? 'PUT' : 'POST',
            url: 'http://localhost:1212/v1/namespaces/cardapp/collections/placements/' + ($(card).data('doc-id') || ''),
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify({
                data: {
                    id: $(card).data('doc-id'),
                    type: 'placements',
                    attributes: {
                        bucket: bucket.id,
                        card: $(card).data('id'),
                        position: $(card).index(),
                    }
                }
            })
        }).then(function(resp) {
            $(card).data('doc-id', resp.data.id);
        });
    }

    // Moving function
    function moveToBucket(target){
        var itemtoMove = $('#list .item').first().detach();
        $(target).prepend(itemtoMove);
        updateCounts();
        updateCard(itemtoMove, $(target)[0]);
    }

    function login() {
        if (document.cookie.indexOf('cookie=') < 0) {
            $.ajax({
                method: 'POST',
                url: 'http://localhost:1212/v1/namespaces/cardapp/auth',
                dataType: 'json',
                contentType: 'application/json',
                data: JSON.stringify({data: {
                    type: 'users',
                    attributes: {
                        provider: 'anon',
                    }
                }})
            }).done(function(data) {
                document.cookie = 'cookie=' + data.data.attributes.token;
                init();
            }).fail(function(data) {
                console.log('FAIL');
            });
        } else {
            init();
        }
    }

    function loadCards(page, accu) {
        page = page || 0;
        accu = accu || [];
        return $.ajax({
            method: 'GET',
            url: 'http://localhost:1212/v1/namespaces/cardapp/collections/cards?limit=50&page=' + page,
            dataType: 'json',
            contentType: 'application/json',
        }).then(function(data) {
            if (data.data.length > 0) {
                return loadCards(page + 1, accu.concat(data.data));
            }
            return accu;
        });
    }

    function init() {
        loadCards().then(function(cards) {
            $('#list').html(cards.map(function(item){
                return '<div class="item alert alert-success" data-id="'+item.id+'">' + item.attributes.content + '</div>';
            }));

            return $.ajax({
                method: 'GET',
                dataType: 'json',
                contentType: 'application/json',
                url: 'http://localhost:1212/v1/namespaces/cardapp/collections/placements/'
            });
        }).then(function(data) {
            data.data.forEach(function(placement) {
                var card = $('[data-id=' + placement.attributes.card + ']').detach();
                card.data('doc-id', placement.id);
                card.data('position', placement.attributes.position);
                $('#' + placement.attributes.bucket).prepend(card);
            });
            updateCounts();
        });
   }

    // Moving with sortable
    $('.bucket').sortable({
        connectWith : '.bucket',
        update: function(e, event){
            if (event.item.parent()[0] != this) return;
            updateCard(event.item, this);
        }
    });

    // Move with clicks
    $('.moveButton').click(function(){
        var el = $(this);
        var target = $(this).attr('data-target');
        moveToBucket(target);
    });

    // Move with arrows
    $('body').keyup(function(){
        // left
        if ( event.which == 37 ) {
            moveToBucket('#bucket1');
        }
        // down
        if ( event.which == 40 ) {
            moveToBucket('#bucket2');
        }
        // right
        if ( event.which == 39 ) {
            moveToBucket('#bucket3');
        }
    });

    // Output data in continue
    $('#continueButton').click(function(){
        var buckets  = {
            1 : [],
            2 : [],
            3 : []
        };
        var bucketOutput = "";

        for(var i = 1; i < 4; i++){
            $('#bucket' + i + ' .item').each(function(){
                buckets[i].push($(this).attr('data-id'));
            });
            bucketOutput += '<div>Bucket ' + i + ': ' + buckets[i].toString() + '</div>';
        }
        $('.bucket-wrap').html(bucketOutput);
    });

    login();
});
